"""Разовая авторизация для tg_activity.py: печатает SESSION_STRING.
Нужен ОТДЕЛЬНЫЙ аккаунт, добавленный админом в отслеживаемые каналы."""
import os

from telethon.sessions import StringSession
from telethon.sync import TelegramClient

with TelegramClient(StringSession(), int(os.environ["API_ID"]), os.environ["API_HASH"]) as c:
    print("\nSESSION_STRING=" + c.session.save())
