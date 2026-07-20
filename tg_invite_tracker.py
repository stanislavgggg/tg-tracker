"""
Telegram Invite Link Tracker + Slack daily report + Google Sheets export
========================================================================
Трекает вступления/выходы в канал по каждой invite-ссылке (SQLite),
раз в день постит сводку в Slack и дописывает строки в Google Sheets.
Опционально шлёт S2S postback в Keitaro при каждом join.

Требования:
    pip install aiogram aiohttp gspread google-auth

Настройка Telegram:
    1. Создай бота через @BotFather, получи токен.
    2. Добавь бота АДМИНОМ в канал (право "Invite Users via Link").
    3. Заполни BOT_TOKEN, CHANNEL_ID, ADMIN_USER_IDS.

Настройка Slack:
    1. Slack -> Apps -> Incoming Webhooks -> создать webhook для нужного канала.
    2. Вставь URL в SLACK_WEBHOOK_URL.

Настройка Google Sheets:
    1. console.cloud.google.com -> создать Service Account -> ключ JSON.
    2. Включи Google Sheets API в проекте.
    3. Скачанный JSON положи рядом со скриптом (GOOGLE_CREDS_FILE).
    4. Расшарь таблицу на email сервис-аккаунта (Editor).
    5. Вставь ID таблицы (из URL) в GSHEET_ID.

Команды боту в личку (только ADMIN_USER_IDS):
    /newlink propeller_lv_cr3   — создать именованную invite-ссылку
    /newlink_req kadam_es_cr1   — ссылка с join request (автоапрув)
    /links                      — список ссылок
    /stats [дней]               — сводка в чат
    /report                     — прогнать дневной отчёт вручную (Slack + Sheets)
"""

import asyncio
import logging
import sqlite3
from datetime import datetime, timedelta, timezone
from zoneinfo import ZoneInfo

import aiohttp
from aiogram import Bot, Dispatcher
from aiogram.filters import Command
from aiogram.types import ChatJoinRequest, ChatMemberUpdated, Message

# ================== CONFIG ==================
BOT_TOKEN = "PASTE_YOUR_BOT_TOKEN"
CHANNEL_ID = -1001234567890          # ID канала (бот должен быть админом)
ADMIN_USER_IDS = {123456789}         # твой user_id (узнать: @userinfobot)

# --- Slack ---
SLACK_WEBHOOK_URL = None             # "https://hooks.slack.com/services/XXX/YYY/ZZZ"

# --- Google Sheets ---
GSHEET_ID = None                     # ID таблицы из URL
GOOGLE_CREDS_FILE = "service_account.json"
SHEET_TAB = "TG Joins"               # вкладка; создастся сама, если нет

# --- Расписание отчёта ---
REPORT_TZ = ZoneInfo("Europe/Madrid")   # часовой пояс отчёта
REPORT_HOUR = 9                          # каждый день в 09:00 — отчёт за вчера

# --- Keitaro S2S postback (None чтобы выключить) ---
KEITARO_POSTBACK_URL = None
# KEITARO_POSTBACK_URL = "https://your-keitaro.com/postback?campaign={source}&status=join&subid_tg={tg_user_id}"

DB_PATH = "tg_tracker.db"
AUTO_APPROVE_JOIN_REQUESTS = True
# ============================================

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
log = logging.getLogger("tg-tracker")


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
            name        TEXT,
            is_request  INTEGER DEFAULT 0,
            created_at  TEXT
        );
        CREATE TABLE IF NOT EXISTS events (
            id          INTEGER PRIMARY KEY AUTOINCREMENT,
            ts          TEXT,               -- UTC ISO
            event       TEXT,               -- join | leave | request
            user_id     INTEGER,
            username    TEXT,
            invite_link TEXT,
            link_name   TEXT
        );
        CREATE INDEX IF NOT EXISTS idx_events_link ON events(link_name);
        CREATE INDEX IF NOT EXISTS idx_events_ts   ON events(ts);
        """)


def link_name_for(invite_link: str | None) -> str:
    if not invite_link:
        return "unknown/organic"
    with db() as conn:
        row = conn.execute(
            "SELECT name FROM links WHERE invite_link = ?", (invite_link,)
        ).fetchone()
    return row["name"] if row else invite_link


def log_event(event: str, user_id: int, username: str | None,
              invite_link: str | None, link_name: str):
    with db() as conn:
        conn.execute(
            "INSERT INTO events (ts, event, user_id, username, invite_link, link_name) "
            "VALUES (?, ?, ?, ?, ?, ?)",
            (datetime.now(timezone.utc).isoformat(), event,
             user_id, username, invite_link, link_name),
        )


def stats_between(start_utc: datetime, end_utc: datetime) -> list[sqlite3.Row]:
    with db() as conn:
        return conn.execute(
            """
            SELECT link_name,
                   SUM(event = 'join')    AS joins,
                   SUM(event = 'leave')   AS leaves,
                   SUM(event = 'request') AS requests
            FROM events
            WHERE ts >= ? AND ts < ?
            GROUP BY link_name
            ORDER BY joins DESC
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
def _sheets_append_sync(rows: list[list]):
    """Синхронная запись через gspread; вызывается из to_thread."""
    import gspread
    from google.oauth2.service_account import Credentials

    creds = Credentials.from_service_account_file(
        GOOGLE_CREDS_FILE,
        scopes=["https://www.googleapis.com/auth/spreadsheets"],
    )
    gc = gspread.authorize(creds)
    sh = gc.open_by_key(GSHEET_ID)
    try:
        ws = sh.worksheet(SHEET_TAB)
    except gspread.WorksheetNotFound:
        ws = sh.add_worksheet(SHEET_TAB, rows=1000, cols=10)
        ws.append_row(["date", "link_name", "joins", "leaves", "net", "requests"],
                      value_input_option="RAW")
    ws.append_rows(rows, value_input_option="RAW")


async def export_to_sheets(rows: list[list]):
    if not GSHEET_ID:
        log.info("GSHEET_ID not configured, skipping")
        return
    try:
        await asyncio.to_thread(_sheets_append_sync, rows)
        log.info("Sheets: appended %d rows", len(rows))
    except Exception as e:
        log.warning("Sheets export failed: %s", e)


# ---------- Дневной отчёт ----------
async def run_daily_report(report_date: datetime.date | None = None):
    """Отчёт за календарный день report_date (по REPORT_TZ). По умолчанию — вчера."""
    now_local = datetime.now(REPORT_TZ)
    if report_date is None:
        report_date = (now_local - timedelta(days=1)).date()

    start_local = datetime.combine(report_date, datetime.min.time(), tzinfo=REPORT_TZ)
    end_local = start_local + timedelta(days=1)
    rows = stats_between(start_local.astimezone(timezone.utc),
                         end_local.astimezone(timezone.utc))

    date_str = report_date.isoformat()

    if not rows:
        await post_to_slack(f":chart_with_upwards_trend: *TG Tracker — {date_str}*\n"
                            f"Событий за день не было.")
        return

    total_j = sum(r["joins"] or 0 for r in rows)
    total_l = sum(r["leaves"] or 0 for r in rows)

    lines = [f":chart_with_upwards_trend: *TG Tracker — {date_str}*",
             f"Всего: *+{total_j} / -{total_l}* (net {total_j - total_l:+d})", ""]
    sheet_rows = []
    for r in rows:
        j, l, req = r["joins"] or 0, r["leaves"] or 0, r["requests"] or 0
        lines.append(f"• `{r['link_name']}`: +{j} / -{l} (net {j - l:+d})"
                     + (f", req: {req}" if req else ""))
        sheet_rows.append([date_str, r["link_name"], j, l, j - l, req])

    await post_to_slack("\n".join(lines))
    await export_to_sheets(sheet_rows)


async def daily_report_scheduler():
    """Ждёт до REPORT_HOUR по REPORT_TZ и запускает отчёт за вчера. Каждый день."""
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


@dp.chat_member()
async def on_member_update(update: ChatMemberUpdated):
    if update.chat.id != CHANNEL_ID:
        return
    old, new = update.old_chat_member.status, update.new_chat_member.status
    user = update.new_chat_member.user
    joined = old in ("left", "kicked") and new in ("member", "administrator")
    left = old in ("member", "administrator") and new in ("left", "kicked")

    if joined:
        raw = update.invite_link.invite_link if update.invite_link else None
        name = link_name_for(raw)
        log_event("join", user.id, user.username, raw, name)
        log.info("JOIN %s (@%s) via %s", user.id, user.username, name)
        await fire_postback(name, user.id)
    elif left:
        with db() as conn:
            row = conn.execute(
                "SELECT link_name, invite_link FROM events "
                "WHERE user_id = ? AND event IN ('join','request') "
                "ORDER BY ts DESC LIMIT 1", (user.id,)
            ).fetchone()
        name = row["link_name"] if row else "unknown/organic"
        raw = row["invite_link"] if row else None
        log_event("leave", user.id, user.username, raw, name)
        log.info("LEAVE %s (@%s) attributed to %s", user.id, user.username, name)


@dp.chat_join_request()
async def on_join_request(req: ChatJoinRequest):
    if req.chat.id != CHANNEL_ID:
        return
    raw = req.invite_link.invite_link if req.invite_link else None
    name = link_name_for(raw)
    log_event("request", req.from_user.id, req.from_user.username, raw, name)
    if AUTO_APPROVE_JOIN_REQUESTS:
        try:
            await req.approve()
            log.info("Approved join request %s via %s", req.from_user.id, name)
        except Exception as e:
            log.warning("Approve failed: %s", e)


@dp.message(Command("newlink"))
async def cmd_newlink(msg: Message):
    if not is_admin(msg):
        return
    parts = msg.text.split(maxsplit=1)
    if len(parts) < 2:
        await msg.answer("Использование: /newlink propeller_lv_cr3")
        return
    name = parts[1].strip()[:32]
    link = await bot.create_chat_invite_link(CHANNEL_ID, name=name)
    with db() as conn:
        conn.execute("INSERT OR REPLACE INTO links VALUES (?, ?, 0, ?)",
                     (link.invite_link, name, datetime.now(timezone.utc).isoformat()))
    await msg.answer(f"Ссылка «{name}»:\n{link.invite_link}")


@dp.message(Command("newlink_req"))
async def cmd_newlink_req(msg: Message):
    if not is_admin(msg):
        return
    parts = msg.text.split(maxsplit=1)
    if len(parts) < 2:
        await msg.answer("Использование: /newlink_req kadam_es_cr1")
        return
    name = parts[1].strip()[:32]
    link = await bot.create_chat_invite_link(CHANNEL_ID, name=name,
                                             creates_join_request=True)
    with db() as conn:
        conn.execute("INSERT OR REPLACE INTO links VALUES (?, ?, 1, ?)",
                     (link.invite_link, name, datetime.now(timezone.utc).isoformat()))
    await msg.answer(f"Join-request ссылка «{name}»:\n{link.invite_link}")


@dp.message(Command("links"))
async def cmd_links(msg: Message):
    if not is_admin(msg):
        return
    with db() as conn:
        rows = conn.execute("SELECT * FROM links ORDER BY created_at DESC").fetchall()
    if not rows:
        await msg.answer("Ссылок пока нет. Создай через /newlink <имя>")
        return
    lines = [f"• {r['name']}{' (req)' if r['is_request'] else ''}\n  {r['invite_link']}"
             for r in rows]
    await msg.answer("\n".join(lines))


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
        await msg.answer("Событий пока нет.")
        return
    period = f"за {days} дн." if days else "за всё время"
    lines = [f"📊 Статистика {period}:\n"]
    for r in rows:
        j, l = r["joins"] or 0, r["leaves"] or 0
        lines.append(f"• {r['link_name']}: +{j} / -{l} (net {j - l:+d})"
                     + (f", req: {r['requests']}" if r["requests"] else ""))
    await msg.answer("\n".join(lines))


@dp.message(Command("report"))
async def cmd_report(msg: Message):
    """Ручной прогон дневного отчёта (за вчера) — Slack + Sheets."""
    if not is_admin(msg):
        return
    await msg.answer("Запускаю отчёт за вчера…")
    await run_daily_report()
    await msg.answer("Готово. Проверь Slack и Google Sheets.")


async def main():
    init_db()
    log.info("Bot started. DB: %s", DB_PATH)
    asyncio.create_task(daily_report_scheduler())
    await dp.start_polling(
        bot,
        allowed_updates=["message", "chat_member", "chat_join_request"],
    )


if __name__ == "__main__":
    asyncio.run(main())
