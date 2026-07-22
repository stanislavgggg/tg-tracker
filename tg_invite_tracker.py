"""
Telegram Invite Link Tracker — multi-channel, env-config (Railway-ready)
========================================================================
Трекает вступления/выходы по каждой invite-ссылке в НЕСКОЛЬКИХ каналах,
шлёт дневной отчёт в Slack и дописывает строки в Google Sheets.

Зависимости (requirements.txt):
    aiogram
    aiohttp
    gspread
    google-auth

Env-переменные (Railway -> Variables):
    BOT_TOKEN               токен бота (обязательно)
    CHANNEL_IDS             ID каналов через запятую: -1003729676193,-1003237183860
    ADMIN_USER_IDS          user_id админов через запятую
    SLACK_WEBHOOK_URL       (опц.) Incoming Webhook для дневного отчёта
    GSHEET_ID               (опц.) ID Google-таблицы
    GOOGLE_CREDS_JSON       (опц.) содержимое service_account.json ЦЕЛИКОМ
    GOOGLE_CREDS_FILE       (опц.) либо путь к файлу ключа (default service_account.json)
    SHEET_TAB               (опц.) вкладка, default "TG Joins"
    REPORT_TZ               (опц.) default "Europe/Madrid"
    REPORT_HOUR             (опц.) default 9
    KEITARO_POSTBACK_URL    (опц.) шаблон с {source} и {tg_user_id}
    DB_PATH                 (опц.) default "tg_tracker.db"
                            !! на Railway укажи путь на volume, напр. /data/tg_tracker.db

Команды боту в личку (только админы):
    /channels                        — список отслеживаемых каналов
    /newlink propeller_lv_cr3        — ссылка в первом (единственном) канале
    /newlink propeller_lv_cr3 -100X  — ссылка в конкретном канале
    /newlink_req kadam_es_cr1 [-100X]— ссылка с join request (автоапрув)
    /addlink name https://t.me/+xxx [chat_id] — зарегистрировать СТАРУЮ ручную
                                     ссылку под читаемым именем (переименует и прошлые события)
    /links                           — все ссылки по каналам
    /stats [дней]                    — сводка по каналам и ссылкам
    /members                         — общее число подписчиков в каждом канале
    /today                           — сводка за сегодня прямо в чат
    /report                          — отчёт за вчера (Slack + Sheets + снапшот Members)
    /report today                    — сводка за сегодня в Slack
    /report 7d | 30d | 90d           — агрегат за последние N дней в Slack
    /report 2026-07-19               — отчёт за конкретную дату (Slack + Sheets)

Периодический intraday-отчёт: env INTRADAY_HOURS=2 -> каждые 2 часа
сводка за текущий день в Slack (0 или не задано = выключено).
"""

import asyncio
import json
import logging
import os
import sqlite3
from datetime import datetime, timedelta, timezone
from zoneinfo import ZoneInfo

import aiohttp
from aiogram import Bot, Dispatcher
from aiogram.filters import Command
from aiogram.types import ChatJoinRequest, ChatMemberUpdated, Message

# ================== CONFIG (env) ==================
BOT_TOKEN = os.environ["BOT_TOKEN"]

CHANNEL_IDS: set[int] = {
    int(x.strip()) for x in os.environ.get("CHANNEL_IDS", "").split(",") if x.strip()
}
ADMIN_USER_IDS: set[int] = {
    int(x.strip()) for x in os.environ.get("ADMIN_USER_IDS", "").split(",") if x.strip()
}

SLACK_WEBHOOK_URL = os.environ.get("SLACK_WEBHOOK_URL") or None
GSHEET_ID = os.environ.get("GSHEET_ID") or None
GOOGLE_CREDS_JSON = os.environ.get("GOOGLE_CREDS_JSON") or None
GOOGLE_CREDS_FILE = os.environ.get("GOOGLE_CREDS_FILE", "service_account.json")
SHEET_TAB = os.environ.get("SHEET_TAB", "TG Joins")

REPORT_TZ = ZoneInfo(os.environ.get("REPORT_TZ", "Europe/Madrid"))
REPORT_HOUR = int(os.environ.get("REPORT_HOUR", "9"))

KEITARO_POSTBACK_URL = os.environ.get("KEITARO_POSTBACK_URL") or None
INTRADAY_HOURS = int(os.environ.get("INTRADAY_HOURS", "0"))  # 0 = выкл; напр. 2 = каждые 2 часа
DB_PATH = os.environ.get("DB_PATH", "tg_tracker.db")
AUTO_APPROVE_JOIN_REQUESTS = os.environ.get("AUTO_APPROVE", "1") == "1"
# ==================================================

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
log = logging.getLogger("tg-tracker")

if not CHANNEL_IDS:
    raise SystemExit("CHANNEL_IDS не задан. Пример: -1003729676193,-1003237183860")

CHANNEL_TITLES: dict[int, str] = {}  # заполняется на старте


# ---------- DB ----------
def db() -> sqlite3.Connection:
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    return conn


def init_db():
    with db() as conn:
        conn.executescript("""
        CREATE TABLE IF NOT EXISTS links (
            invite_link TEXT PRIMARY KEY,
            chat_id     INTEGER,
            name        TEXT,
            is_request  INTEGER DEFAULT 0,
            created_at  TEXT
        );
        CREATE TABLE IF NOT EXISTS events (
            id          INTEGER PRIMARY KEY AUTOINCREMENT,
            ts          TEXT,               -- UTC ISO
            chat_id     INTEGER,
            event       TEXT,               -- join | leave | request
            user_id     INTEGER,
            username    TEXT,
            invite_link TEXT,
            link_name   TEXT
        );
        CREATE INDEX IF NOT EXISTS idx_events_chat ON events(chat_id);
        CREATE INDEX IF NOT EXISTS idx_events_ts   ON events(ts);
        CREATE TABLE IF NOT EXISTS known_chats (
            chat_id  INTEGER PRIMARY KEY,
            title    TEXT,
            status   TEXT,
            seen_at  TEXT
        );
        """)


def link_name_for(invite_link: str | None) -> str:
    if not invite_link:
        return "unknown/organic"
    with db() as conn:
        row = conn.execute(
            "SELECT name FROM links WHERE invite_link = ?", (invite_link,)
        ).fetchone()
    return row["name"] if row else invite_link


def log_event(chat_id: int, event: str, user_id: int, username: str | None,
              invite_link: str | None, link_name: str):
    with db() as conn:
        conn.execute(
            "INSERT INTO events (ts, chat_id, event, user_id, username, invite_link, link_name) "
            "VALUES (?, ?, ?, ?, ?, ?, ?)",
            (datetime.now(timezone.utc).isoformat(), chat_id, event,
             user_id, username, invite_link, link_name),
        )


def chan_label(chat_id: int) -> str:
    return CHANNEL_TITLES.get(chat_id) or str(chat_id)


def stats_between(start_utc: datetime, end_utc: datetime) -> list[sqlite3.Row]:
    with db() as conn:
        return conn.execute(
            """
            SELECT chat_id, link_name,
                   SUM(event = 'join')    AS joins,
                   SUM(event = 'leave')   AS leaves,
                   SUM(event = 'request') AS requests
            FROM events
            WHERE ts >= ? AND ts < ?
            GROUP BY chat_id, link_name
            ORDER BY chat_id, joins DESC
            """,
            (start_utc.isoformat(), end_utc.isoformat()),
        ).fetchall()


# ---------- Keitaro postback ----------
async def fire_postback(source: str, tg_user_id: int):
    if not KEITARO_POSTBACK_URL:
        return
    url = KEITARO_POSTBACK_URL.format(source=source, tg_user_id=tg_user_id)
    try:
        async with aiohttp.ClientSession() as s:
            async with s.get(url, timeout=aiohttp.ClientTimeout(total=10)) as r:
                log.info("Postback %s -> %s", url, r.status)
    except Exception as e:
        log.warning("Postback failed: %s", e)


# ---------- Slack ----------
async def post_to_slack(text: str):
    if not SLACK_WEBHOOK_URL:
        log.info("Slack webhook not configured, skipping")
        return
    try:
        async with aiohttp.ClientSession() as s:
            async with s.post(SLACK_WEBHOOK_URL, json={"text": text},
                              timeout=aiohttp.ClientTimeout(total=10)) as r:
                log.info("Slack post -> %s", r.status)
    except Exception as e:
        log.warning("Slack post failed: %s", e)


# ---------- Google Sheets ----------
def _sheets_append_sync(tab: str, header: list[str], rows: list[list]):
    import gspread
    from google.oauth2.service_account import Credentials

    scopes = ["https://www.googleapis.com/auth/spreadsheets"]
    if GOOGLE_CREDS_JSON:
        creds = Credentials.from_service_account_info(
            json.loads(GOOGLE_CREDS_JSON), scopes=scopes)
    else:
        creds = Credentials.from_service_account_file(GOOGLE_CREDS_FILE, scopes=scopes)

    gc = gspread.authorize(creds)
    sh = gc.open_by_key(GSHEET_ID)
    try:
        ws = sh.worksheet(tab)
    except gspread.WorksheetNotFound:
        ws = sh.add_worksheet(tab, rows=1000, cols=10)
        ws.append_row(header, value_input_option="RAW")
    ws.append_rows(rows, value_input_option="RAW")


async def export_to_sheets(rows: list[list], tab: str | None = None,
                           header: list[str] | None = None):
    if not GSHEET_ID:
        log.info("GSHEET_ID not configured, skipping")
        return
    tab = tab or SHEET_TAB
    header = header or ["date", "channel", "link_name", "joins", "leaves", "net", "requests"]
    try:
        await asyncio.to_thread(_sheets_append_sync, tab, header, rows)
        log.info("Sheets [%s]: appended %d rows", tab, len(rows))
    except Exception as e:
        log.warning("Sheets export failed: %s", e)


# ---------- Report rendering ----------
async def get_member_counts() -> dict[int, int]:
    """Current total subscriber count per tracked channel (live from Telegram)."""
    counts = {}
    for cid in CHANNEL_IDS:
        try:
            counts[cid] = await bot.get_chat_member_count(cid)
        except Exception as e:
            log.warning("Member count failed for %s: %s", cid, e)
    return counts


def short_link_label(name: str) -> str:
    """Named links stay as-is; unnamed raw URLs get truncated with a marker."""
    if name.startswith("https://t.me/"):
        return "unnamed:" + name.removeprefix("https://t.me/")[:12] + "…"
    return name


def render_report(rows, title: str, member_counts: dict[int, int] | None = None) -> str:
    """Slack message: per-channel monospace tables + grand total."""
    member_counts = member_counts or {}
    lines = [f"*{title}*"]

    if not rows:
        lines.append("No join/leave events recorded in this period.")
    by_chat: dict[int, list] = {}
    for r in rows:
        by_chat.setdefault(r["chat_id"], []).append(r)

    total_j = total_l = total_req = 0
    for chat_id, chat_rows in by_chat.items():
        c_j = sum(r["joins"] or 0 for r in chat_rows)
        c_l = sum(r["leaves"] or 0 for r in chat_rows)
        c_req = sum(r["requests"] or 0 for r in chat_rows)
        total_j, total_l, total_req = total_j + c_j, total_l + c_l, total_req + c_req

        members = member_counts.get(chat_id)
        members_str = f" · members now: {members:,}" if members is not None else ""
        lines.append(f"\n:loudspeaker: *{chan_label(chat_id)}* — "
                     f"joins {c_j}, left {c_l}, net {c_j - c_l:+d}{members_str}")
        name_w = max([len(short_link_label(r["link_name"])) for r in chat_rows] + [4])
        table = [f"{'link'.ljust(name_w)}  joins  left   net"]
        for r in chat_rows:
            j, l = r["joins"] or 0, r["leaves"] or 0
            table.append(f"{short_link_label(r['link_name']).ljust(name_w)}"
                         f"  {str(j).rjust(5)}  {str(l).rjust(4)}  {f'{j - l:+d}'.rjust(4)}")
        lines.append("```" + "\n".join(table) + "```")

    # channels with no events this period still get a members line
    silent = [cid for cid in member_counts if cid not in by_chat]
    if silent:
        lines.append("")
        for cid in silent:
            lines.append(f":zzz: *{chan_label(cid)}* — no events"
                         f" · members now: {member_counts[cid]:,}")

    if rows:
        lines.append(f"\n*TOTAL: joins {total_j}, left {total_l}, "
                     f"net {total_j - total_l:+d}*"
                     + (f" (join requests: {total_req})" if total_req else ""))
    return "\n".join(lines)


# ---------- Daily report ----------
async def run_daily_report(report_date=None):
    now_local = datetime.now(REPORT_TZ)
    if report_date is None:
        report_date = (now_local - timedelta(days=1)).date()

    start_local = datetime.combine(report_date, datetime.min.time(), tzinfo=REPORT_TZ)
    end_local = start_local + timedelta(days=1)
    rows = stats_between(start_local.astimezone(timezone.utc),
                         end_local.astimezone(timezone.utc))
    date_str = report_date.isoformat()
    counts = await get_member_counts()

    await post_to_slack(render_report(
        rows, f":chart_with_upwards_trend: TG Tracker — daily report, {date_str}",
        counts))

    if rows:
        sheet_rows = [[date_str, chan_label(r["chat_id"]), r["link_name"],
                       r["joins"] or 0, r["leaves"] or 0,
                       (r["joins"] or 0) - (r["leaves"] or 0), r["requests"] or 0]
                      for r in rows]
        await export_to_sheets(sheet_rows)

    # daily snapshot of total members per channel -> "Members" tab
    if counts:
        snapshot_date = now_local.date().isoformat()
        member_rows = [[snapshot_date, chan_label(cid), n]
                       for cid, n in counts.items()]
        await export_to_sheets(member_rows, tab="Members",
                               header=["date", "channel", "members"])


async def build_period_report(days: int) -> str:
    """Aggregated report for the last N days (including today so far)."""
    end = datetime.now(timezone.utc)
    start = end - timedelta(days=days)
    rows = stats_between(start, end)
    counts = await get_member_counts()
    return render_report(
        rows, f":bar_chart: TG Tracker — last {days} day(s), "
              f"up to {datetime.now(REPORT_TZ).strftime('%Y-%m-%d %H:%M')}",
        counts)


async def build_today_summary() -> str:
    """Stats for today: from local midnight (REPORT_TZ) until now."""
    now_local = datetime.now(REPORT_TZ)
    start_local = now_local.replace(hour=0, minute=0, second=0, microsecond=0)
    rows = stats_between(start_local.astimezone(timezone.utc),
                         now_local.astimezone(timezone.utc))
    counts = await get_member_counts()
    return render_report(
        rows, f":hourglass_flowing_sand: TG Tracker — today so far, "
              f"{now_local.strftime('%Y-%m-%d %H:%M')}",
        counts)


async def intraday_report_scheduler():
    """Posts a today-so-far summary to Slack every INTRADAY_HOURS hours."""
    while True:
        await asyncio.sleep(INTRADAY_HOURS * 3600)
        try:
            await post_to_slack(await build_today_summary())
        except Exception as e:
            log.exception("Intraday report failed: %s", e)


async def daily_report_scheduler():
    while True:
        now = datetime.now(REPORT_TZ)
        next_run = now.replace(hour=REPORT_HOUR, minute=0, second=0, microsecond=0)
        if next_run <= now:
            next_run += timedelta(days=1)
        wait = (next_run - now).total_seconds()
        log.info("Next daily report at %s (in %.0f min)", next_run, wait / 60)
        await asyncio.sleep(wait)
        try:
            await run_daily_report()
        except Exception as e:
            log.exception("Daily report failed: %s", e)


# ---------- Bot ----------
bot = Bot(BOT_TOKEN)
dp = Dispatcher()


def is_admin(msg: Message) -> bool:
    return msg.from_user and msg.from_user.id in ADMIN_USER_IDS


def parse_target_channel(arg: str | None) -> int | None:
    """Определяет канал: явный ID из команды или единственный канал по умолчанию."""
    if arg:
        try:
            cid = int(arg)
            return cid if cid in CHANNEL_IDS else None
        except ValueError:
            return None
    if len(CHANNEL_IDS) == 1:
        return next(iter(CHANNEL_IDS))
    return None


@dp.chat_member()
async def on_member_update(update: ChatMemberUpdated):
    if update.chat.id not in CHANNEL_IDS:
        return
    old, new = update.old_chat_member.status, update.new_chat_member.status
    user = update.new_chat_member.user
    joined = old in ("left", "kicked") and new in ("member", "administrator")
    left = old in ("member", "administrator") and new in ("left", "kicked")

    if joined:
        raw = update.invite_link.invite_link if update.invite_link else None
        name = link_name_for(raw)
        log_event(update.chat.id, "join", user.id, user.username, raw, name)
        log.info("JOIN %s (@%s) via %s in %s", user.id, user.username, name,
                 chan_label(update.chat.id))
        await fire_postback(name, user.id)
    elif left:
        with db() as conn:
            row = conn.execute(
                "SELECT link_name, invite_link FROM events "
                "WHERE user_id = ? AND chat_id = ? AND event IN ('join','request') "
                "ORDER BY ts DESC LIMIT 1", (user.id, update.chat.id)
            ).fetchone()
        name = row["link_name"] if row else "unknown/organic"
        raw = row["invite_link"] if row else None
        log_event(update.chat.id, "leave", user.id, user.username, raw, name)
        log.info("LEAVE %s (@%s) attributed to %s in %s", user.id, user.username,
                 name, chan_label(update.chat.id))


@dp.chat_join_request()
async def on_join_request(req: ChatJoinRequest):
    if req.chat.id not in CHANNEL_IDS:
        return
    raw = req.invite_link.invite_link if req.invite_link else None
    name = link_name_for(raw)
    log_event(req.chat.id, "request", req.from_user.id, req.from_user.username, raw, name)
    if AUTO_APPROVE_JOIN_REQUESTS:
        try:
            await req.approve()
            log.info("Approved join request %s via %s", req.from_user.id, name)
        except Exception as e:
            log.warning("Approve failed: %s", e)


@dp.message(Command("channels"))
async def cmd_channels(msg: Message):
    if not is_admin(msg):
        return
    lines = [f"• {chan_label(cid)} — `{cid}`" for cid in CHANNEL_IDS]
    await msg.answer("Tracked channels:\n" + "\n".join(lines), parse_mode="Markdown")


async def _create_link(msg: Message, is_request: bool):
    parts = msg.text.split()
    if len(parts) < 2:
        cmd = "/newlink_req" if is_request else "/newlink"
        await msg.answer(f"Usage: {cmd} link_name [chat_id]\n"
                         f"chat_id is required when tracking multiple channels — see /channels")
        return
    name = parts[1].strip()[:32]
    chat_id = parse_target_channel(parts[2] if len(parts) > 2 else None)
    if chat_id is None:
        await msg.answer("Couldn't resolve the target channel. "
                         "Pass its chat_id — see /channels")
        return
    link = await bot.create_chat_invite_link(chat_id, name=name,
                                             creates_join_request=is_request)
    with db() as conn:
        conn.execute("INSERT OR REPLACE INTO links VALUES (?, ?, ?, ?, ?)",
                     (link.invite_link, chat_id, name, int(is_request),
                      datetime.now(timezone.utc).isoformat()))
    kind = "Join-request link" if is_request else "Link"
    await msg.answer(f"{kind} \"{name}\" for {chan_label(chat_id)}:\n{link.invite_link}")


@dp.message(Command("newlink"))
async def cmd_newlink(msg: Message):
    if is_admin(msg):
        await _create_link(msg, is_request=False)


@dp.message(Command("newlink_req"))
async def cmd_newlink_req(msg: Message):
    if is_admin(msg):
        await _create_link(msg, is_request=True)


@dp.message(Command("addlink"))
async def cmd_addlink(msg: Message):
    """Register an EXISTING manually created invite link under a readable name,
    so reports show the name instead of the raw URL.
    Usage: /addlink propeller_lv_old1 https://t.me/+Kmq4hCIFxQs1YTQ0 [chat_id]"""
    if not is_admin(msg):
        return
    parts = msg.text.split()
    if len(parts) < 3 or not parts[2].startswith("https://t.me/"):
        await msg.answer("Usage: /addlink name https://t.me/+xxxx [chat_id]")
        return
    name = parts[1].strip()[:32]
    url = parts[2].strip()
    chat_id = parse_target_channel(parts[3] if len(parts) > 3 else None)
    if chat_id is None:
        await msg.answer("Couldn't resolve the target channel. "
                         "Pass its chat_id — see /channels")
        return
    with db() as conn:
        conn.execute("INSERT OR REPLACE INTO links VALUES (?, ?, ?, 0, ?)",
                     (url, chat_id, name, datetime.now(timezone.utc).isoformat()))
        # rename the link in already recorded events too
        conn.execute("UPDATE events SET link_name = ? WHERE invite_link = ?",
                     (name, url))
    await msg.answer(f"Registered \"{name}\" for {chan_label(chat_id)}.\n"
                     f"Past and future events for this link will show this name.")


@dp.message(Command("links"))
async def cmd_links(msg: Message):
    if not is_admin(msg):
        return
    with db() as conn:
        rows = conn.execute(
            "SELECT * FROM links ORDER BY chat_id, created_at DESC").fetchall()
    if not rows:
        await msg.answer("No links yet. Create one with /newlink <name> [chat_id] "
                         "or register an existing one with /addlink")
        return
    lines, current = [], None
    for r in rows:
        if r["chat_id"] != current:
            current = r["chat_id"]
            lines.append(f"\n{chan_label(current)}:")
        lines.append(f"• {r['name']}{' (req)' if r['is_request'] else ''}\n  {r['invite_link']}")
    await msg.answer("\n".join(lines).strip())


@dp.message(Command("stats"))
async def cmd_stats(msg: Message):
    if not is_admin(msg):
        return
    parts = msg.text.split()
    days = int(parts[1]) if len(parts) > 1 and parts[1].isdigit() else None
    end = datetime.now(timezone.utc)
    start = end - timedelta(days=days) if days else datetime(2000, 1, 1, tzinfo=timezone.utc)
    rows = stats_between(start, end)
    if not rows:
        await msg.answer("No events recorded yet.")
        return
    period = f"last {days} day(s)" if days else "all time"
    lines, current = [f"📊 Stats — {period}:"], None
    for r in rows:
        if r["chat_id"] != current:
            current = r["chat_id"]
            lines.append(f"\n{chan_label(current)}:")
        j, l = r["joins"] or 0, r["leaves"] or 0
        lines.append(f"• {short_link_label(r['link_name'])}: +{j} / -{l} (net {j - l:+d})"
                     + (f", req: {r['requests']}" if r["requests"] else ""))
    await msg.answer("\n".join(lines))


@dp.message(Command("members"))
async def cmd_members(msg: Message):
    """Current total subscriber count per channel."""
    if not is_admin(msg):
        return
    counts = await get_member_counts()
    if not counts:
        await msg.answer("Couldn't fetch member counts. Is the bot admin everywhere?")
        return
    lines = ["👥 Current members:"]
    for cid, n in sorted(counts.items(), key=lambda x: -x[1]):
        lines.append(f"• {chan_label(cid)}: {n:,}")
    lines.append(f"\nTotal across channels: {sum(counts.values()):,}")
    await msg.answer("\n".join(lines))


@dp.message(Command("today"))
async def cmd_today(msg: Message):
    """Today-so-far summary sent directly to this chat (no Slack/Sheets)."""
    if not is_admin(msg):
        return
    text = (await build_today_summary()).replace("*", "").replace("```", "")
    await msg.answer(text.replace(":hourglass_flowing_sand: ", "⏳ ")
                         .replace(":loudspeaker: ", "📢 ")
                         .replace(":zzz: ", "💤 ")
                         .replace(":chart_with_upwards_trend: ", "📈 "))


@dp.message(Command("report"))
async def cmd_report(msg: Message):
    """/report — yesterday (Slack+Sheets); /report today — today-so-far to Slack;
    /report 7d — last 7 days to Slack; /report 2026-07-19 — specific date."""
    if not is_admin(msg):
        return
    parts = msg.text.split()
    arg = parts[1].lower() if len(parts) > 1 else None

    if arg == "today":
        await post_to_slack(await build_today_summary())
        await msg.answer("Today's summary sent to Slack.")
        return

    # period syntax: 7d, 30d, 90d ...
    if arg and arg.endswith("d") and arg[:-1].isdigit():
        await post_to_slack(await build_period_report(int(arg[:-1])))
        await msg.answer(f"Last {arg[:-1]} days summary sent to Slack.")
        return

    report_date = None
    if arg:
        try:
            report_date = datetime.strptime(arg, "%Y-%m-%d").date()
        except ValueError:
            await msg.answer("Couldn't parse the argument. Formats: /report, "
                             "/report today, /report 7d, /report 2026-07-19")
            return

    await msg.answer("Running the report…")
    await run_daily_report(report_date)
    await msg.answer("Done. Check Slack and Google Sheets.")


HELP_TEXT = """Available commands:

Links
/newlink name [chat_id] — create a tracked invite link
/newlink_req name [chat_id] — same, but with join request (auto-approve, filters bots)
/addlink name https://t.me/+xxx [chat_id] — register an existing manual link under a name
/links — list all registered links per channel

Stats
/today — today-so-far summary (here, in chat)
/stats [days] — quick per-link stats (all time or last N days)
/members — current subscriber totals per channel
/channels — tracked channels and their chat IDs

Reports to Slack
/report — yesterday's report (Slack + Google Sheets)
/report today — today-so-far to Slack
/report 7d | 30d — aggregated period report to Slack
/report 2026-07-19 — report for a specific date

Automatic: daily report at {rh}:00 ({tz}), intraday every {ih}h.
"""


@dp.message(Command("help", "start"))
async def cmd_help(msg: Message):
    if not is_admin(msg):
        return
    await msg.answer(HELP_TEXT.format(
        rh=REPORT_HOUR, tz=str(REPORT_TZ),
        ih=INTRADAY_HOURS if INTRADAY_HOURS else "off"))


@dp.my_chat_member()
async def on_bot_membership_change(update: ChatMemberUpdated):
    """Fires when the bot itself is added/removed/promoted in any chat.
    Remembers every channel so /discover can list them with IDs."""
    chat = update.chat
    status = update.new_chat_member.status
    with db() as conn:
        conn.execute(
            "INSERT OR REPLACE INTO known_chats VALUES (?, ?, ?, ?)",
            (chat.id, chat.title or chat.username or str(chat.id), status,
             datetime.now(timezone.utc).isoformat()))
    log.info("Bot membership change: %s (%s) -> %s", chat.title, chat.id, status)


@dp.message(Command("discover"))
async def cmd_discover(msg: Message):
    """All channels where the bot has been added, with a ready-to-paste
    CHANNEL_IDS string (admin-status chats only)."""
    if not is_admin(msg):
        return
    with db() as conn:
        rows = conn.execute(
            "SELECT * FROM known_chats ORDER BY seen_at DESC").fetchall()
    if not rows:
        await msg.answer(
            "No channels discovered yet. Add the bot as ADMIN to your channels — "
            "each one will appear here automatically. Then run /discover again.")
        return

    admin_ids = []
    lines = ["Channels the bot has been added to:\n"]
    for r in rows:
        tracked = " ✅ tracked" if r["chat_id"] in CHANNEL_IDS else ""
        if r["status"] == "administrator":
            admin_ids.append(str(r["chat_id"]))
            lines.append(f"• {r['title']} — {r['chat_id']} (admin){tracked}")
        else:
            lines.append(f"• {r['title']} — {r['chat_id']} ({r['status']}, "
                         f"needs admin!){tracked}")

    if admin_ids:
        lines.append("\nReady-to-paste CHANNEL_IDS value (admin chats only):")
        lines.append(",".join(admin_ids))
    await msg.answer("\n".join(lines))


async def resolve_channel_titles():
    for cid in CHANNEL_IDS:
        try:
            chat = await bot.get_chat(cid)
            CHANNEL_TITLES[cid] = chat.title or str(cid)
        except Exception as e:
            log.warning("Could not fetch channel %s: %s (is the bot an admin there?)",
                        cid, e)


async def main():
    init_db()
    await resolve_channel_titles()
    from aiogram.types import BotCommand
    await bot.set_my_commands([
        BotCommand(command="help", description="List all commands"),
        BotCommand(command="today", description="Today-so-far summary"),
        BotCommand(command="members", description="Current subscribers per channel"),
        BotCommand(command="stats", description="Per-link stats (/stats 7 = last 7 days)"),
        BotCommand(command="report", description="Send report to Slack (/report today, 7d, date)"),
        BotCommand(command="newlink", description="Create tracked invite link"),
        BotCommand(command="newlink_req", description="Create join-request link"),
        BotCommand(command="addlink", description="Register existing manual link"),
        BotCommand(command="links", description="List registered links"),
        BotCommand(command="channels", description="Tracked channels + chat IDs"),
        BotCommand(command="discover", description="List all channels bot was added to (with IDs)"),
    ])
    log.info("Bot started. Channels: %s. DB: %s",
             ", ".join(chan_label(c) for c in CHANNEL_IDS), DB_PATH)
    asyncio.create_task(daily_report_scheduler())
    if INTRADAY_HOURS > 0:
        log.info("Intraday Slack report enabled: every %d h", INTRADAY_HOURS)
        asyncio.create_task(intraday_report_scheduler())
    await dp.start_polling(
        bot,
        allowed_updates=["message", "chat_member", "chat_join_request",
                         "my_chat_member"],
    )


if __name__ == "__main__":
    asyncio.run(main())
