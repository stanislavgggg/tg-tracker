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
  4. Старых людей (вступивших до запуска, уже не в окне) добираем по публичному username из events
     (contacts.resolveUsername отдаёт и ключ, и статус) — понемногу за запуск из-за лимитов Telegram.
  5. Склеиваем с events: человек -> ссылка и неделя вступления. Пишем три вкладки в тот же Google-файл:
        Activity Channels  — по дням и каналам
        Activity Sources   — по дням, каналам и ссылкам
        Cohorts            — ссылка × неделя вступления (читаемый вид, перезаписывается целиком)
        Summary            — по каждой ссылке: когорта прошлой недели против этой
        How to read        — инструкция внутри файла
Охват честный: в каждой строке есть checked (у скольких знаем статус) и coverage_%.
Считаются люди, вступившие с момента запуска модуля (+ окно на момент первого скана).

Env (если не заданы API_ID / API_HASH / SESSION_STRING — модуль выключен):
    API_ID, API_HASH        my.telegram.org
    SESSION_STRING          python activity_login.py (отдельный аккаунт, админ каналов)
    ACTIVITY_HOUR           час ежедневной перепроверки и записи в Sheets, REPORT_TZ, default 10
    ACTIVITY_SCAN_HOURS     как часто читать окно новых подписчиков, default 6
    ACTIVITY_BACKFILL_PER_RUN  сколько старых людей добирать по username за ежедневный запуск, default 150
    ACTIVITY_MIN_CHECKED    «Data quality = OK» только если известен статус минимум у стольких людей, default 20
    ACTIVITY_MIN_COVERAGE   ...и охват не ниже N процентов, default 50
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
BACKFILL_PER_RUN = int(os.environ.get("ACTIVITY_BACKFILL_PER_RUN", "150"))
# «Data quality = OK»: достаточно людей с известным статусом и достаточный охват (пороги выбраны мной, настраиваются)
MIN_CHECKED = int(os.environ.get("ACTIVITY_MIN_CHECKED", "20"))
MIN_COVERAGE = float(os.environ.get("ACTIVITY_MIN_COVERAGE", "50"))
STAY_DAYS = 7  # «Stayed 7d»: не вышел из канала в течение 7 дней после вступления
# Недельная когорта «созрела», когда с конца её недели прошло 7 дней: раньше «активен за 7 дней» верно для всех
# просто потому, что человек вступил недавно (чтобы вступить, надо быть в Telegram).
MATURE_AFTER_DAYS = 14  # от понедельника недели вступления

ORGANIC = "unknown/organic"  # то же имя, что использует трекер
NO_JOIN = "no join record"
CHANNEL_TAB = "Activity Channels"
SOURCE_TAB = "Activity Sources"
COHORT_TAB = "Cohorts"
SUMMARY_TAB = "Summary"
HOWTO_TAB = "How to read"

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

COHORT_HEADER = ["Channel", "Link", "Joined in week starting (Mon)", "Joined", "Still in channel",
                 "Still in %", "Stayed 7d %", "Status known %", "Active 7d %", "Active 30d %",
                 "vs channel avg (pp)", "Data quality"]
SUMMARY_FIXED = ["Channel", "Link"]

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
    CREATE TABLE IF NOT EXISTS username_tried (   -- попытки добора по username
        user_id INTEGER PRIMARY KEY,
        ts      INTEGER NOT NULL,
        ok      INTEGER NOT NULL
    );
    CREATE TABLE IF NOT EXISTS kv (
        key   TEXT PRIMARY KEY,
        value TEXT
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


async def _backfill_usernames(client, db_path, channel_ids, limit):
    """Добирает ключи и статусы старых людей по публичному username (из events).
    Только для тех, кто сейчас в канале и ещё не в tg_users. Останавливается на FloodWait: продолжим завтра."""
    from telethon import types
    from telethon.errors import FloodWaitError
    if limit <= 0:
        return 0
    conn = sqlite3.connect(db_path)
    now0 = int(time.time())
    kv = dict(conn.execute("SELECT key, value FROM kv WHERE key IN ('backfill_block_until','backfill_last')"))
    if now0 < int(kv.get("backfill_block_until", 0)):
        conn.close()
        log.info("Backfill: Telegram ограничил запросы по username, пропускаю до %s UTC",
                 datetime.fromtimestamp(int(kv["backfill_block_until"]), timezone.utc).strftime("%Y-%m-%d %H:%M"))
        return 0
    if now0 - int(kv.get("backfill_last", 0)) < 20 * 3600:  # не жечь лимит при повторных /activity
        conn.close()
        log.info("Backfill: уже запускался меньше 20 часов назад, пропускаю (лимит Telegram на username)")
        return 0
    members = members_by_chat(conn, set(channel_ids))
    have = {r[0] for r in conn.execute("SELECT user_id FROM tg_users WHERE access_hash IS NOT NULL")}
    retry_after = int(time.time()) - 30 * 86400
    tried = {r[0] for r in conn.execute("SELECT user_id FROM username_tried WHERE ts > ?", (retry_after,))}
    uname, last_ts = {}, {}
    for uid, un, ts in conn.execute(
            "SELECT user_id, username, ts FROM events WHERE username IS NOT NULL AND username != '' ORDER BY ts"):
        uname[uid], last_ts[uid] = un, ts
    conn.close()
    want = set().union(*members.values()) if members else set()
    todo = [u for u in want if u in uname and u not in have and u not in tried]
    todo.sort(key=lambda u: last_ts[u], reverse=True)  # сначала самые свежие
    now, done = int(time.time()), 0
    conn = sqlite3.connect(db_path)
    with conn:
        conn.execute("INSERT OR REPLACE INTO kv VALUES('backfill_last', ?)", (str(now),))
    conn.close()
    for uid in todo[:limit]:
        ok, row = 0, None
        try:
            ent = await client.get_entity(uname[uid])
            if isinstance(ent, types.User) and ent.id == uid:
                b, seen = bucket_of(ent.status, now)
                row = (uid, ent.access_hash, int(bool(ent.bot)), int(bool(ent.deleted)), b, seen, now, now)
                ok = 1
        except FloodWaitError as e:
            log.warning("Backfill: FloodWait %ss, остановлено, продолжим при следующем запуске (добрано %d)",
                        e.seconds, done)
            conn = sqlite3.connect(db_path)
            with conn:
                conn.execute("INSERT OR REPLACE INTO kv VALUES('backfill_block_until', ?)",
                             (str(int(time.time()) + e.seconds + 120),))
            conn.close()
            break
        except Exception:
            pass  # username сменился / не найден — пометим и не будем пробовать 30 дней
        conn = sqlite3.connect(db_path)
        with conn:
            if row:
                conn.execute(
                    """INSERT INTO tg_users(user_id, access_hash, is_bot, is_deleted, bucket,
                                            last_seen_ts, status_ts, first_seen_ts) VALUES(?,?,?,?,?,?,?,?)
                       ON CONFLICT(user_id) DO UPDATE SET access_hash=excluded.access_hash,
                         bucket=excluded.bucket, last_seen_ts=excluded.last_seen_ts, status_ts=excluded.status_ts""", row)
            conn.execute("INSERT OR REPLACE INTO username_tried VALUES(?,?,?)", (uid, now, ok))
        conn.close()
        done += ok
        await asyncio.sleep(2.5)
    log.info("Backfill по username: добрано %d из %d кандидатов (лимит за запуск %d)", done, len(todo), limit)
    return done


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


def _week_start(ts_iso: str) -> str:
    d = datetime.fromisoformat(ts_iso).astimezone(timezone.utc).date()
    return (d - timedelta(days=d.weekday())).isoformat()  # понедельник недели вступления


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
    # --- ссылка × неделя вступления (читаемый вид) ---
    avg = {}  # пулованная доля активных 7д по каналу, для колонки «vs channel avg»
    for chat_id in chat_ids:
        b = [status[u] for u in members.get(chat_id, ()) if u in status]
        if b:
            avg[chat_id] = pct(sum(1 for x in b if x in ACTIVE_7D), len(b))
    # Stayed 7d по событиям трекера: считаем только тех, кто вступил не менее 7 дней назад
    last_join, leaves = {}, {}
    for chat_id, user_id, ev, _, ts in events:
        if ev == "join":
            last_join[(chat_id, user_id)] = ts
        elif ev == "leave":
            leaves.setdefault((chat_id, user_id), []).append(ts)
    now_iso = datetime.now(timezone.utc)
    stay_cut = (now_iso - timedelta(days=STAY_DAYS)).isoformat()
    mature_week_max = (now_iso.date() - timedelta(days=MATURE_AFTER_DAYS)).isoformat()
    coh = {}
    for k, (name, ts) in attr.items():
        g = coh.setdefault((k[0], name, _week_start(ts)), {"joined": 0, "now": set(), "m": 0, "stayed": 0})
        if k in joined:
            g["joined"] += 1
            jt = last_join.get(k)
            if jt and jt <= stay_cut:
                g["m"] += 1
                jt_end = (datetime.fromisoformat(jt) + timedelta(days=STAY_DAYS)).isoformat()
                if not any(jt < lt <= jt_end for lt in leaves.get(k, [])):
                    g["stayed"] += 1
        if k[1] in members.get(k[0], ()):
            g["now"].add(k[1])
    for chat_id, mem in members.items():
        for u in mem:
            if (chat_id, u) not in attr:
                coh.setdefault((chat_id, ORGANIC, NO_JOIN), {"joined": 0, "now": set(), "m": 0, "stayed": 0})["now"].add(u)
    coh_rows = []
    for (chat_id, name, wk), g in sorted(coh.items(), key=lambda kv: (label_fn(kv[0][0]), kv[0][1], kv[0][2])):
        now_u, jn = g["now"], g["joined"]
        if not jn and not now_u:
            continue
        b = [status[u] for u in now_u if u in status]
        nn, n = len(now_u), len(b)
        cov = pct(n, nn)
        fresh = wk != NO_JOIN and wk > mature_week_max
        ok = n >= MIN_CHECKED and cov >= MIN_COVERAGE and not fresh
        a7 = pct(sum(1 for x in b if x in ACTIVE_7D), n)
        a30 = pct(sum(1 for x in b if x in ACTIVE_30D), n)
        vs = round(a7 - avg[chat_id], 1) if ok and chat_id in avg else ""
        stay = pct(g["stayed"], g["m"]) if g["m"] >= MIN_CHECKED else ""
        quality = "Too fresh" if fresh else ("OK" if ok else "Low data")
        coh_rows.append([label_fn(chat_id), name, wk, jn, nn,
                         pct(nn, jn) if jn and nn <= jn else "", stay, cov, a7, a30, vs, quality])
    return ch_rows, src_rows, coh_rows


def summary_weeks():
    """Две последние «созревшие» недели вступления: (старшая, младшая)."""
    today = datetime.now(timezone.utc).date()
    newer = today - timedelta(days=MATURE_AFTER_DAYS)
    newer = newer - timedelta(days=newer.weekday())  # понедельник последней созревшей недели
    return (newer - timedelta(days=7)).isoformat(), newer.isoformat()


def summary_header(old_wk, new_wk):
    return ["Channel", "Link",
            f"Joined (week {old_wk})", f"Stayed 7d % ({old_wk})", f"Active 7d % ({old_wk})",
            f"Joined (week {new_wk})", f"Stayed 7d % ({new_wk})", f"Active 7d % ({new_wk})",
            "Change: stayed (pp)", "Change: active (pp)", "Data quality"]


def build_summary(coh_rows):
    """Для каждой ссылки: две последние созревшие недели вступления рядом (честное сравнение одного возраста)."""
    old_wk, new_wk = summary_weeks()
    by = {}
    for r in coh_rows:
        if r[2] in (old_wk, new_wk):
            by.setdefault((r[0], r[1]), {})[r[2]] = r
    out = []
    for (ch, link), d in sorted(by.items()):
        o, n = d.get(old_wk), d.get(new_wk)
        ok = bool(o and n and o[11] == "OK" and n[11] == "OK")
        dstay = round(n[6] - o[6], 1) if o and n and o[6] != "" and n[6] != "" else ""
        dact = round(n[8] - o[8], 1) if ok else ""
        out.append([ch, link,
                    o[3] if o else 0, o[6] if o else "", o[8] if o else "",
                    n[3] if n else 0, n[6] if n else "", n[8] if n else "",
                    dstay, dact, "OK" if ok else "Low data"])
    return out


def howto_rows():
    return [[l] for l in [
        "WHAT THIS IS",
        "For every invite link we look at the people who joined through it: did they stay, and do they still use Telegram. "
        "One invite link = one approach, so each row is the traffic quality of one approach in one join week.",
        "",
        "WHERE TO LOOK",
        "Summary: each link, the two latest mature weeks side by side. Start here.",
        "Cohorts: every link x every join week. Use the filter on the header row (channel, link).",
        "Activity Channels / Activity Sources: daily detail per channel and per link, for deeper digging.",
        "",
        "WHY A WEEK'S TRAFFIC CAN BE JUDGED ONLY ~2 WEEKS LATER",
        "A person who joined in the last 7 days was in Telegram in the last 7 days by definition, so 'Active 7d' is meaningless for fresh cohorts "
        "(it shows ~100% for everyone). A week's cohort becomes comparable 14 days after the week starts. Until then the row is marked Too fresh and greyed out.",
        "'Stayed 7d' (did not unsubscribe within 7 days after joining) is the quick signal: it is computed from join/leave events for everyone, with no Telegram lookup, "
        "and it is the same age for every cohort, so it is a fair comparison.",
        "",
        "HOW TO READ A ROW (tab Cohorts)",
        "Joined: people who joined through this link in that week. Still in channel / Still in %: how many of them are still subscribed now.",
        "Stayed 7d %: of those who joined at least 7 days ago, the share that did not leave within 7 days. Blank if there are too few people.",
        "Status known %: for what share of the people still in the channel we know their last-seen status. This is the reliability of the Active columns.",
        "Active 7d % / Active 30d %: share of known people who were in Telegram in the last 7 / 30 days.",
        "vs channel avg (pp): this cohort's Active 7d % minus the whole channel's Active 7d %, in percentage points. Green = better than the channel, red = worse.",
        "Data quality: OK = enough people with known status (at least %d, and at least %d%% of them known) and the cohort is mature. Low data or Too fresh rows are greyed out: do not draw conclusions from them." % (MIN_CHECKED, MIN_COVERAGE),
        "",
        "HOW TO COMPARE FAIRLY",
        "Compare neighbouring mature weeks of the same link, or different links in the same week.",
        "Keep the approach (landing, creative, audience) unchanged within a link. If the approach changes, create a new invite link, otherwise the cohort is mixed.",
        "",
        "LIMITS",
        "Some people hide last seen: Telegram then gives only a rough bucket (recently / within a week / within a month / long ago). These are included, with their bucket.",
        "Active in Telegram does not mean reads our channel.",
        "People who joined before the tracker started are only partly visible. Coverage grows over the first days.",
        "'no join record' in the week column = subscribers we have no join event for (organic or joined before the tracker).",
        "Weeks are by the date of the person's last join event, in UTC.",
    ]]


# ---------- оформление вкладок ----------
GREEN, RED, GREY = (0.85, 0.95, 0.85), (0.98, 0.84, 0.84), (0.6, 0.6, 0.6)


def _view_cohorts():
    from tg_sheets_format import col_letter as L
    q, v = L(11), L(10)
    return dict(
        ncols=12, index=2, tab_color=(0.2, 0.6, 0.35), freeze_cols=2, filter=True,
        widths=[210, 200, 130, 80, 90, 85, 90, 95, 90, 90, 110, 100],
        percent_cols=[5, 6, 7, 8, 9], pp_cols=[10],
        notes=["Telegram channel.", "Invite link name (one link = one approach).",
               "Monday of the week the people joined.",
               "People who joined via this link in that week.",
               "How many of them are still subscribed now.",
               "Still in channel / Joined. Blank if it is not reliable (more people than joined).",
               "Of those who joined at least 7 days ago: share that did not leave within 7 days. Same age for every cohort, so comparable. Blank if too few people.",
               "For what share of the people still in the channel we know last seen. Low = do not trust the Active columns.",
               "Share of known people in Telegram within the last 7 days. Meaningless for cohorts that joined in the last 7 days.",
               "Same for 30 days.",
               "This cohort's Active 7d % minus the whole channel's. Green = better than channel, red = worse. Only for OK rows.",
               "OK = enough known people, enough coverage and a mature cohort. Too fresh = joined less than 14 days ago (Active is not comparable yet). Low data = too few known people."],
        rules=[dict(formula=f'=OR(${q}2="Low data",${q}2="Too fresh")', c0=0, c1=12, fg=GREY),
               dict(formula=f'=AND(${q}2="OK",${v}2>0)', c0=10, c1=11, bg=GREEN),
               dict(formula=f'=AND(${q}2="OK",${v}2<0)', c0=10, c1=11, bg=RED)])


def _view_summary():
    return dict(
        ncols=11, index=1, tab_color=(0.2, 0.45, 0.8), freeze_cols=2, filter=True, header_height=64,
        widths=[210, 200, 100, 100, 100, 100, 100, 100, 105, 105, 100],
        percent_cols=[3, 4, 6, 7], pp_cols=[8, 9],
        notes=["Telegram channel.", "Invite link name.",
               "People who joined via this link in the older of the two compared weeks.",
               "Of those, share that did not leave within 7 days after joining.",
               "Share of known people from that cohort in Telegram in the last 7 days (the cohort is mature, so this is meaningful).",
               "People who joined via this link in the newer of the two compared weeks.",
               "Same: stayed 7 days.", "Same: active in the last 7 days.",
               "Newer minus older, in percentage points. Green = newer week is better.",
               "Newer minus older, in percentage points. Only if both weeks have OK data.",
               "OK only if both weeks have enough known people. Otherwise Low data."],
        rules=[dict(formula='=$K2="Low data"', c0=0, c1=11, fg=GREY),
               dict(formula='=AND($I2<>"",$I2>0)', c0=8, c1=9, bg=GREEN),
               dict(formula='=AND($I2<>"",$I2<0)', c0=8, c1=9, bg=RED),
               dict(formula='=AND($K2="OK",$J2>0)', c0=9, c1=10, bg=GREEN),
               dict(formula='=AND($K2="OK",$J2<0)', c0=9, c1=10, bg=RED)])


def _view_howto():
    return dict(ncols=1, index=0, tab_color=(0.95, 0.75, 0.2), freeze_cols=0, widths=[980], wrap_all=True,
                header_height=34)


def _view_plain(header, tab_color=(0.6, 0.6, 0.6)):
    return dict(ncols=len(header), tab_color=tab_color, freeze_cols=2, filter=True,
                widths=[100, 210] + [105] * (len(header) - 2),
                percent_cols=[i for i, h in enumerate(header) if "%" in h])


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
            backfilled = await _backfill_usernames(client, db_path, channel_ids, BACKFILL_PER_RUN)
            conn = sqlite3.connect(db_path)
            members = members_by_chat(conn, set(channel_ids))
            conn.close()
            all_members = set().union(*members.values()) if members else set()
            rechecked = await _recheck(client, db_path, all_members, started)
    ch_rows, src_rows, coh_rows = await asyncio.to_thread(build_rows, db_path, day, label_fn, set(channel_ids))
    await export_fn(howto_rows(), tab=HOWTO_TAB, header=["How to read this file"], replace_all=True,
                    view=_view_howto())
    ow, nw = summary_weeks()
    await export_fn(build_summary(coh_rows), tab=SUMMARY_TAB, header=summary_header(ow, nw), replace_all=True,
                    view=_view_summary())
    await export_fn(coh_rows, tab=COHORT_TAB, header=COHORT_HEADER, replace_all=True, view=_view_cohorts())
    await export_fn(ch_rows, tab=CHANNEL_TAB, header=CHANNEL_HEADER, replace_date=day,
                    view=_view_plain(CHANNEL_HEADER))
    await export_fn(src_rows, tab=SOURCE_TAB, header=SOURCE_HEADER, replace_date=day,
                    view=_view_plain(SOURCE_HEADER))
    lines = [f"📶 Активность подписчиков, {day} (перепроверено поимённо: {rechecked}, добрано по username: {backfilled}):"]
    for r in ch_rows:
        lines.append(f"• {r[1]}: отслеживается {r[2]:,}, охват {r[4]}%, активны 7д {r[15]}%, 30д {r[17]}%")
    if failed:
        lines.append("\n⚠️ Окно не снято: " + "; ".join(f"{label_fn(c)} — {why}" for c, why in failed.items()))
    return "\n".join(lines)
