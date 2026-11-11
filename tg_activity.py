"""
Активность подписчиков (last seen) — модуль для tg_invite_tracker.py
====================================================================
Проблема: Telegram отдаёт список подписчиков канала только примерно по ~200 человек.
Решение: копим людей, а не берём список целиком.
  1. Несколько раз в день читаем «окно» (последние ~200 подписчиков каждого канала) и для каждого
     человека сохраняем user_id и ключ доступа (access_hash). Каждый новый подписчик попадает в окно,
     пока он свежий, так что мы его «ловим».
  2. Раз в сутки перепроверяем last seen у всех накопленных людей ПОИМЁННО (users.getUsers по
     сохранённым ключам) — в том числе у тех, кто давно вышел из окна.
  3. Кто сейчас в канале, берём из событий трекера (таблица events: join/leave) плюс текущее окно.
  4. Склеиваем с events: человек -> ссылка, по которой вступил. Пишем две вкладки в тот же Google-файл:
        Activity Channels  — по дням и каналам
        Activity Sources   — по дням, каналам и ссылкам (когорты по ссылкам)
Охват честный: в каждой строке есть checked (у скольких знаем статус) и coverage_%.
Считаются люди, вступившие с момента запуска модуля (+ окно на момент первого скана).

Env (если не заданы API_ID / API_HASH / SESSION_STRING — модуль выключен):
    API_ID, API_HASH        my.telegram.org
    SESSION_STRING          python activity_login.py (отдельный аккаунт, админ каналов)
    ACTIVITY_HOUR           час ежедневной перепроверки и записи в Sheets, REPORT_TZ, default 10
    ACTIVITY_SCAN_HOURS     как часто читать окно новых подписчиков, default 6
    ACTIVITY_WINDOW_DAYS    в Activity Sources учитывать вступивших за последние N дней, 0 = за всё время
"""
import asyncio
import logging
import os
import sqlite3
import time
from datetime import datetime, timedelta, timezone

log = logging.getLogger("tg-activity")

API_ID = os.environ.get("API_ID") or None
API_HASH = os.environ.get("API_HASH") or None
SESSION_STRING = os.environ.get("SESSION_STRING") or None
ENABLED = bool(API_ID and API_HASH and SESSION_STRING)
ACTIVITY_HOUR = int(os.environ.get("ACTIVITY_HOUR", "10"))
SCAN_HOURS = float(os.environ.get("ACTIVITY_SCAN_HOURS", "6"))
WINDOW_DAYS = int(os.environ.get("ACTIVITY_WINDOW_DAYS", "0"))

ORGANIC = "unknown/organic"  # то же имя, что использует трекер
CHANNEL_TAB = "Activity Channels"
SOURCE_TAB = "Activity Sources"

# «активен» — определение можно менять здесь
ACTIVE_7D = {"online", "d1", "d7", "h_recently", "h_week"}
ACTIVE_30D = ACTIVE_7D | {"d30", "h_month"}
BUCKETS = ["online", "d1", "d7", "d30", "older", "h_recently", "h_week", "h_month", "h_long"]

CHANNEL_HEADER = ["date", "channel", "tracked_members", "checked", "coverage_%",
                  "online", "seen_1d", "seen_7d", "seen_30d", "seen_older",
                  "hidden_recently", "hidden_week", "hidden_month", "hidden_long",
                  "active_7d", "active_7d_%", "active_30d", "active_30d_%", "hidden_%"]
SOURCE_HEADER = ["date", "channel", "link_name", "joined", "in_channel_now", "retained_%",
                 "checked", "coverage_%", "active_7d", "active_7d_%_of_checked",
                 "active_7d_%_of_joined", "active_30d", "active_30d_%_of_checked", "hidden_%_of_checked"]

_lock = None  # один Telegram-коннект за раз: два одновременных клиента на одной сессии её ломают


def _get_lock():
    global _lock
    if _lock is None:
        _lock = asyncio.Lock()
    return _lock


def pct(x, n):
    return round(100 * x / n, 1) if n else 0


def bucket_of(status, now=None):
    """(корзина, unix last_seen | None). Точное время есть только если статус открыт."""
    from telethon import types
    now = now or datetime.now(timezone.utc).timestamp()
    if isinstance(status, types.UserStatusOnline):
        return "online", int(now)
    if isinstance(status, types.UserStatusOffline):
        ts = int(status.was_online.timestamp())
        age = now - ts
        for name, lim in (("d1", 86400), ("d7", 7 * 86400), ("d30", 30 * 86400)):
            if age <= lim:
                return name, ts
        return "older", ts
    if isinstance(status, types.UserStatusRecently):
        return "h_recently", None
    if isinstance(status, types.UserStatusLastWeek):
        return "h_week", None
    if isinstance(status, types.UserStatusLastMonth):
        return "h_month", None
    return "h_long", None


def init_tables(conn: sqlite3.Connection):
    conn.executescript("""
    CREATE TABLE IF NOT EXISTS tg_users (
        user_id      INTEGER PRIMARY KEY,
        access_hash  INTEGER,
        is_bot       INTEGER NOT NULL DEFAULT 0,
        is_deleted   INTEGER NOT NULL DEFAULT 0,
        bucket       TEXT,               -- последняя известная корзина
        last_seen_ts INTEGER,
        status_ts    INTEGER,            -- когда статус обновлён
        first_seen_ts INTEGER
    );
    CREATE TABLE IF NOT EXISTS chat_window (   -- кто был в последнем окне канала
        chat_id INTEGER NOT NULL,
        user_id INTEGER NOT NULL,
        PRIMARY KEY (chat_id, user_id)
    );
    CREATE TABLE IF NOT EXISTS chat_scan (
        chat_id INTEGER PRIMARY KEY,
        scan_ts INTEGER,
        n       INTEGER
    );
    """)


# ---------- причины ошибок человеческим языком ----------
def _reason(e: Exception) -> str:
    name = type(e).__name__
    if name == "ChatAdminRequiredError":
        return "аккаунт не админ в канале"
    if isinstance(e, ValueError) and "input entity" in str(e):
        return "аккаунт не видит канал (не в нём)"
    return f"{name}"


# ---------- сбор из Telegram ----------
async def _scan(client, channel_ids, db_path, label_fn):
    """Читает окно каждого канала, копит людей и ключи. Возвращает (done, failed)."""
    from telethon.errors import FloodWaitError
    done, failed = {}, {}
    now = int(time.time())
    for cid in channel_ids:
        for attempt in (1, 2):
            try:
                ent = await client.get_entity(cid)
                users = []
                async for u in client.iter_participants(ent):  # «недавние», потолок ~200 у Telegram
                    users.append(u)
                conn = sqlite3.connect(db_path)
                with conn:
                    prev = conn.execute("SELECT 1 FROM chat_scan WHERE chat_id=?", (cid,)).fetchone()
                    known = {r[0] for r in conn.execute("SELECT user_id FROM tg_users")}
                    new_n = sum(1 for u in users if u.id not in known)
                    for u in users:
                        b, seen = bucket_of(u.status, now)
                        conn.execute(
                            """INSERT INTO tg_users(user_id, access_hash, is_bot, is_deleted, bucket,
                                                    last_seen_ts, status_ts, first_seen_ts)
                               VALUES(?,?,?,?,?,?,?,?)
                               ON CONFLICT(user_id) DO UPDATE SET access_hash=excluded.access_hash,
                                 is_bot=excluded.is_bot, is_deleted=excluded.is_deleted,
                                 bucket=excluded.bucket, last_seen_ts=excluded.last_seen_ts,
                                 status_ts=excluded.status_ts""",
                            (u.id, u.access_hash, int(bool(u.bot)), int(bool(u.deleted)), b, seen, now, now))
                    conn.execute("DELETE FROM chat_window WHERE chat_id=?", (cid,))
                    conn.executemany("INSERT INTO chat_window VALUES(?,?)", [(cid, u.id) for u in users])
                    conn.execute("INSERT OR REPLACE INTO chat_scan VALUES(?,?,?)", (cid, now, len(users)))
                conn.close()
                done[cid] = len(users)
                if prev and len(users) >= 150 and new_n >= 0.75 * len(users):
                    log.warning("%s: за период окно сменилось почти целиком (%d новых из %d) — "
                                "часть вступивших могла не попасть; сканируйте чаще (ACTIVITY_SCAN_HOURS)",
                                label_fn(cid), new_n, len(users))
                break
            except FloodWaitError as e:
                log.warning("FloodWait %ss on %s (attempt %d)", e.seconds, label_fn(cid), attempt)
                await asyncio.sleep(e.seconds + 5)
            except Exception as e:
                log.warning("Activity scan failed for %s [%s]: %s", label_fn(cid), cid, _reason(e))
                failed[cid] = _reason(e)
                break
    return done, failed


async def _get_users(client, pairs):
    """users.getUsers пачкой; при ошибке дробим пополам, чтобы один плохой ключ не рушил всё."""
    from telethon.errors import FloodWaitError, RPCError
    from telethon.tl.functions.users import GetUsersRequest
    from telethon.tl.types import InputUser
    if not pairs:
        return []
    try:
        res = await client(GetUsersRequest([InputUser(uid, h) for uid, h in pairs]))
        return list(res)
    except FloodWaitError as e:
        await asyncio.sleep(e.seconds + 5)
        return await _get_users(client, pairs)
    except (RPCError, ValueError) as e:
        if len(pairs) == 1:
            log.info("getUsers: пропускаю user %s (%s)", pairs[0][0], type(e).__name__)
            return []
        mid = len(pairs) // 2
        return await _get_users(client, pairs[:mid]) + await _get_users(client, pairs[mid:])


async def _recheck(client, db_path, user_ids, skip_fresh_after):
    """Обновляет статусы людей по сохранённым ключам. Свежие (обновлённые в этом запуске) пропускает."""
    from telethon import types
    conn = sqlite3.connect(db_path)
    rows = conn.execute(
        f"SELECT user_id, access_hash FROM tg_users WHERE user_id IN ({','.join('?' * len(user_ids))}) "
        "AND access_hash IS NOT NULL AND (status_ts IS NULL OR status_ts < ?)",
        list(user_ids) + [skip_fresh_after]) if user_ids else []
    todo = list(rows)
    conn.close()
    now = int(time.time())
    updated = 0
    for i in range(0, len(todo), 100):
        batch = todo[i:i + 100]
        users = await _get_users(client, batch)
        upd = []
        for u in users:
            if isinstance(u, types.UserEmpty):
                continue
            b, seen = bucket_of(u.status, now)
            upd.append((b, seen, now, int(bool(u.deleted)), u.id))
        conn = sqlite3.connect(db_path)
        with conn:
            conn.executemany("UPDATE tg_users SET bucket=?, last_seen_ts=?, status_ts=?, is_deleted=? WHERE user_id=?", upd)
        conn.close()
        updated += len(upd)
        await asyncio.sleep(0.5)  # не долбим API
    return updated


# ---------- кто сейчас в канале ----------
def _events(conn, chat_ids):
    q = ("SELECT chat_id, user_id, event, link_name, ts FROM events "
         "WHERE event IN ('join','request','leave') ORDER BY ts")
    return [r for r in conn.execute(q) if r[0] in chat_ids]


def members_by_chat(conn, chat_ids):
    """chat_id -> set(user_id). В канале = последнее событие join, ИЛИ человек в последнем окне канала.
    Без ботов и удалённых."""
    last = {}
    for chat_id, user_id, ev, _, _ in _events(conn, chat_ids):
        last[(chat_id, user_id)] = ev
    out = {c: set() for c in chat_ids}
    for (c, u), ev in last.items():
        if ev == "join":
            out[c].add(u)
    for c, u in conn.execute("SELECT chat_id, user_id FROM chat_window"):
        if c in out:
            out[c].add(u)
    bad = {r[0] for r in conn.execute("SELECT user_id FROM tg_users WHERE is_bot=1 OR is_deleted=1")}
    for c in out:
        out[c] -= bad
    return out


def _attribution(events):
    """(chat, user) -> (link_name, ts). Берём последнюю НЕ-organic ссылку среди join/request
    (у join после автоапрува ссылка может быть пустой), иначе organic. Плюс множество тех, у кого был join."""
    attr, joined = {}, set()
    for chat_id, user_id, ev, name, ts in events:
        if ev == "leave":
            continue
        k = (chat_id, user_id)
        if ev == "join":
            joined.add(k)
        cur = attr.get(k)
        if name and name != ORGANIC:
            attr[k] = (name, ts)
        elif cur is None:
            attr[k] = (ORGANIC, ts)
        elif ev == "join":
            attr[k] = (cur[0], ts)
    return attr, joined


# ---------- агрегаты ----------
def build_rows(db_path: str, day: str, label_fn, chat_ids):
    conn = sqlite3.connect(db_path)
    try:
        status = {uid: b for uid, b in conn.execute(
            "SELECT user_id, bucket FROM tg_users WHERE bucket IS NOT NULL")}
        members = members_by_chat(conn, set(chat_ids))
        events = _events(conn, set(chat_ids))
    finally:
        conn.close()
    attr, joined = _attribution(events)
    cutoff = None
    if WINDOW_DAYS > 0:
        cutoff = (datetime.now(timezone.utc) - timedelta(days=WINDOW_DAYS)).isoformat()

    ch_rows = []
    for chat_id in sorted(chat_ids, key=lambda c: label_fn(c)):
        mem = members.get(chat_id, set())
        if not mem:
            continue
        b = [status[u] for u in mem if u in status]
        n, nm = len(b), len(mem)
        cnt = {k: b.count(k) for k in BUCKETS}
        a7 = sum(cnt[k] for k in ACTIVE_7D)
        a30 = sum(cnt[k] for k in ACTIVE_30D)
        hid = sum(v for k, v in cnt.items() if k.startswith("h_"))
        ch_rows.append([day, label_fn(chat_id), nm, n, pct(n, nm)] + [cnt[k] for k in BUCKETS]
                       + [a7, pct(a7, n), a30, pct(a30, n), pct(hid, n)])

    groups = {}  # (chat, src) -> {"joined": n, "now": set(user)}
    for k, (name, ts) in attr.items():
        if cutoff and ts < cutoff:
            continue
        g = groups.setdefault((k[0], name), {"joined": 0, "now": set()})
        if k in joined:
            g["joined"] += 1
        if k[1] in members.get(k[0], ()):
            g["now"].add(k[1])
    if not cutoff:  # люди в канале без событий трекера: пришли до трекера или без ссылки
        for chat_id, mem in members.items():
            for u in mem:
                if (chat_id, u) not in attr:
                    groups.setdefault((chat_id, ORGANIC), {"joined": 0, "now": set()})["now"].add(u)
    src_rows = []
    for (chat_id, name), g in sorted(groups.items(), key=lambda kv: (label_fn(kv[0][0]), -len(kv[1]["now"]))):
        now_u, jn = g["now"], g["joined"]
        b = [status[u] for u in now_u if u in status]
        nn, n = len(now_u), len(b)
        a7 = sum(1 for x in b if x in ACTIVE_7D)
        a30 = sum(1 for x in b if x in ACTIVE_30D)
        hid = sum(1 for x in b if x.startswith("h_"))
        src_rows.append([day, label_fn(chat_id), name, jn, nn,
                         pct(nn, jn) if jn and nn <= jn else "", n, pct(n, nn), a7, pct(a7, n),
                         pct(a7, jn) if jn else "", a30, pct(a30, n), pct(hid, n)])
    return ch_rows, src_rows


# ---------- публичные точки входа ----------
def _new_client():
    from telethon import TelegramClient
    from telethon.sessions import StringSession
    return TelegramClient(StringSession(SESSION_STRING), int(API_ID), API_HASH)


async def scan_only(channel_ids, db_path, label_fn) -> str:
    """Лёгкий запуск несколько раз в день: пополняет базу людьми из окон каналов."""
    if not ENABLED:
        return "Активность выключена."
    async with _get_lock():
        async with _new_client() as client:
            await client.get_dialogs()  # прогреть кэш сущностей
            done, failed = await _scan(client, sorted(channel_ids), db_path, label_fn)
    log.info("Activity scan: ok=%d failed=%d", len(done), len(failed))
    return f"ok={len(done)} failed={len(failed)}"


async def run(channel_ids, db_path, label_fn, export_fn) -> str:
    """Скан окон + перепроверка всех накопленных людей + запись в Google Sheets."""
    if not ENABLED:
        return "Активность выключена: не заданы API_ID / API_HASH / SESSION_STRING."
    day = datetime.now(timezone.utc).strftime("%Y-%m-%d")
    started = int(time.time())
    async with _get_lock():
        async with _new_client() as client:
            await client.get_dialogs()
            done, failed = await _scan(client, sorted(channel_ids), db_path, label_fn)
            conn = sqlite3.connect(db_path)
            members = members_by_chat(conn, set(channel_ids))
            conn.close()
            all_members = set().union(*members.values()) if members else set()
            rechecked = await _recheck(client, db_path, all_members, started)
    ch_rows, src_rows = await asyncio.to_thread(build_rows, db_path, day, label_fn, set(channel_ids))
    await export_fn(ch_rows, tab=CHANNEL_TAB, header=CHANNEL_HEADER, replace_date=day)
    await export_fn(src_rows, tab=SOURCE_TAB, header=SOURCE_HEADER, replace_date=day)
    lines = [f"📶 Активность подписчиков, {day} (перепроверено поимённо: {rechecked}):"]
    for r in ch_rows:
        lines.append(f"• {r[1]}: отслеживается {r[2]:,}, охват {r[4]}%, активны 7д {r[15]}%, 30д {r[17]}%")
    if failed:
        lines.append("\n⚠️ Окно не снято: " + "; ".join(f"{label_fn(c)} — {why}" for c, why in failed.items()))
    return "\n".join(lines)
