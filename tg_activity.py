"""
Активность подписчиков (last seen) — модуль для tg_invite_tracker.py
====================================================================
Раз в сутки снимает статусы подписчиков каналов через Telethon (user-аккаунт-админ),
склеивает с таблицей events трекера (user_id -> ссылка, по которой человек вступил)
и пишет две вкладки в ТОТ ЖЕ Google-файл:
  Activity            — по дням и каналам: подписчики, корзины, доля активных
  Activity by Source  — по дням, каналам и ссылкам/источникам: сколько вступило,
                        сколько осталось и какая доля из них реально заходит в Telegram

Env (если не заданы API_ID / API_HASH / SESSION_STRING — модуль выключен):
    API_ID, API_HASH       my.telegram.org
    SESSION_STRING         python activity_login.py (отдельный аккаунт, админ каналов)
    ACTIVITY_HOUR          час запуска в REPORT_TZ, default 10
    ACTIVITY_WINDOW_DAYS   в «by Source» учитывать только вступивших за N дней, 0 = за всё время (default 0)
"""
import asyncio
import logging
import os
import sqlite3
from datetime import datetime, timedelta, timezone

log = logging.getLogger("tg-activity")

API_ID = os.environ.get("API_ID") or None
API_HASH = os.environ.get("API_HASH") or None
SESSION_STRING = os.environ.get("SESSION_STRING") or None
ENABLED = bool(API_ID and API_HASH and SESSION_STRING)
ACTIVITY_HOUR = int(os.environ.get("ACTIVITY_HOUR", "10"))
WINDOW_DAYS = int(os.environ.get("ACTIVITY_WINDOW_DAYS", "0"))

ORGANIC = "unknown/organic"  # то же имя, что использует трекер

# «активен» — определение можно менять здесь
ACTIVE_7D = {"online", "d1", "d7", "h_recently", "h_week"}
ACTIVE_30D = ACTIVE_7D | {"d30", "h_month"}
BUCKETS = ["online", "d1", "d7", "d30", "older", "h_recently", "h_week", "h_month", "h_long"]

CHANNEL_HEADER = ["date", "channel", "subscribers", "online", "seen_1d", "seen_7d", "seen_30d",
                  "seen_older", "hidden_recently", "hidden_week", "hidden_month", "hidden_long",
                  "active_7d", "active_7d_%", "active_30d", "active_30d_%", "hidden_%"]
SOURCE_HEADER = ["date", "channel", "link_name", "joined", "in_channel_now", "retained_%",
                 "active_7d", "active_7d_%_of_now", "active_7d_%_of_joined",
                 "active_30d", "active_30d_%_of_now", "hidden_%_of_now"]


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
    CREATE TABLE IF NOT EXISTS activity_snapshots (
        day          TEXT NOT NULL,      -- UTC YYYY-MM-DD
        chat_id      INTEGER NOT NULL,
        user_id      INTEGER NOT NULL,
        bucket       TEXT NOT NULL,
        last_seen_ts INTEGER,
        PRIMARY KEY (day, chat_id, user_id)
    );
    """)


# ---------- сбор из Telegram ----------
async def collect(channel_ids, db_path: str, day: str) -> dict[int, int]:
    """Снимает статусы и пишет в activity_snapshots. Возвращает {chat_id: сколько записано}."""
    from telethon import TelegramClient
    from telethon.errors import FloodWaitError
    from telethon.sessions import StringSession

    done = {}
    async with TelegramClient(StringSession(SESSION_STRING), int(API_ID), API_HASH) as client:
        await client.get_dialogs()  # прогреть кэш, чтобы find по -100... id сработал
        for cid in channel_ids:
            for attempt in (1, 2):
                try:
                    ent = await client.get_entity(cid)
                    rows = []
                    # aggressive=True обходит лимит ~200 на запрос перебором по буквам (медленнее, но полнее)
                    async for u in client.iter_participants(ent, aggressive=True):
                        if u.bot or u.deleted:
                            continue
                        b, seen = bucket_of(u.status)
                        rows.append((day, cid, u.id, b, seen))
                    conn = sqlite3.connect(db_path)
                    with conn:
                        conn.execute("DELETE FROM activity_snapshots WHERE day=? AND chat_id=?", (day, cid))
                        conn.executemany("INSERT INTO activity_snapshots VALUES(?,?,?,?,?)", rows)
                    conn.close()
                    done[cid] = len(rows)
                    break
                except FloodWaitError as e:
                    log.warning("FloodWait %ss on %s (attempt %d)", e.seconds, cid, attempt)
                    await asyncio.sleep(e.seconds + 5)
                except Exception as e:
                    log.warning("Activity snapshot failed for %s: %r", cid, e)
                    break
    return done


# ---------- агрегаты ----------
def _attribution(conn, chat_ids):
    """(chat_id, user_id) -> (link_name, ts последнего join). Берём последнюю НЕ-organic ссылку
    среди join/request (у join после автоапрува ссылка может быть пустой), иначе organic.
    Возвращает также множество пользователей, у которых был именно join."""
    attr, joined = {}, set()
    q = ("SELECT chat_id, user_id, event, link_name, ts FROM events "
         "WHERE event IN ('join','request') ORDER BY ts")
    for chat_id, user_id, ev, name, ts in conn.execute(q):
        if chat_id not in chat_ids:
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
            attr[k] = (cur[0], ts)  # обновить ts вступления, имя оставить прежнее
    return attr, joined


def build_rows(db_path: str, day: str, label_fn):
    conn = sqlite3.connect(db_path)
    try:
        chat_ids = {r[0] for r in conn.execute("SELECT DISTINCT chat_id FROM activity_snapshots WHERE day=?", (day,))}
        snap = {}  # (chat, user) -> bucket
        for chat_id, user_id, b in conn.execute("SELECT chat_id, user_id, bucket FROM activity_snapshots WHERE day=?", (day,)):
            snap[(chat_id, user_id)] = b
        attr, joined = _attribution(conn, chat_ids)
    finally:
        conn.close()

    cutoff = None
    if WINDOW_DAYS > 0:
        cutoff = (datetime.now(timezone.utc) - timedelta(days=WINDOW_DAYS)).isoformat()

    # --- по каналам ---
    ch_rows = []
    per_chat = {}
    for (chat_id, _), b in snap.items():
        per_chat.setdefault(chat_id, []).append(b)
    for chat_id in sorted(per_chat, key=lambda c: label_fn(c)):
        b = per_chat[chat_id]
        n = len(b)
        cnt = {k: b.count(k) for k in BUCKETS}
        a7 = sum(cnt[k] for k in ACTIVE_7D)
        a30 = sum(cnt[k] for k in ACTIVE_30D)
        hid = sum(v for k, v in cnt.items() if k.startswith("h_"))
        ch_rows.append([day, label_fn(chat_id), n] + [cnt[k] for k in BUCKETS]
                       + [a7, pct(a7, n), a30, pct(a30, n), pct(hid, n)])

    # --- по источникам ---
    groups = {}  # (chat, src) -> {"joined": n, "now": [buckets]}
    for k, (name, ts) in attr.items():
        if cutoff and ts < cutoff:
            continue
        g = groups.setdefault((k[0], name), {"joined": 0, "now": []})
        if k in joined:
            g["joined"] += 1
        if k in snap:
            g["now"].append(snap[k])
    for k, b in snap.items():  # подписчики без событий в трекере (пришли до трекера / без ссылки)
        if k not in attr and not cutoff:
            g = groups.setdefault((k[0], ORGANIC), {"joined": 0, "now": []})
            g["now"].append(b)
    src_rows = []
    for (chat_id, name), g in sorted(groups.items(), key=lambda kv: (label_fn(kv[0][0]), -len(kv[1]["now"]))):
        now_b, jn = g["now"], g["joined"]
        n = len(now_b)
        a7 = sum(1 for x in now_b if x in ACTIVE_7D)
        a30 = sum(1 for x in now_b if x in ACTIVE_30D)
        hid = sum(1 for x in now_b if x.startswith("h_"))
        src_rows.append([day, label_fn(chat_id), name, jn, n,
                         pct(n, jn) if jn else "", a7, pct(a7, n),
                         pct(a7, jn) if jn else "", a30, pct(a30, n), pct(hid, n)])
    return ch_rows, src_rows


async def run(channel_ids, db_path, label_fn, export_fn) -> str:
    """Снимок + запись в Google Sheets. Возвращает короткую сводку для чата."""
    if not ENABLED:
        return "Активность выключена: не заданы API_ID / API_HASH / SESSION_STRING."
    day = datetime.now(timezone.utc).strftime("%Y-%m-%d")
    done = await collect(sorted(channel_ids), db_path, day)
    if not done:
        return "Не удалось снять ни один канал (см. логи; аккаунт должен быть админом каналов)."
    ch_rows, src_rows = await asyncio.to_thread(build_rows, db_path, day, label_fn)
    await export_fn(ch_rows, tab="Activity", header=CHANNEL_HEADER, replace_date=day)
    await export_fn(src_rows, tab="Activity by Source", header=SOURCE_HEADER, replace_date=day)
    lines = [f"📶 Активность подписчиков, {day}:"]
    for r in ch_rows:
        lines.append(f"• {r[1]}: {r[2]:,} подп., активны 7д {r[13]}%, 30д {r[15]}%, скрыт last seen {r[16]}%")
    missed = set(channel_ids) - set(done)
    if missed:
        lines.append("Не снят: " + ", ".join(label_fn(c) for c in missed))
    return "\n".join(lines)
