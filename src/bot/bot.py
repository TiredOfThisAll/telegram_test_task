from os import getenv
import json
from datetime import datetime

from aiogram import Bot, Dispatcher
from aiogram.client.default import DefaultBotProperties
from aiogram.enums import ParseMode
from aiogram.types import Message

from data_access.connection import get_collection
from data_access.repository import collect_dataset


TOKEN = getenv("BOT_TOKEN")

bot = Bot(token=TOKEN, default=DefaultBotProperties(parse_mode=ParseMode.HTML))

dp = Dispatcher()


@dp.message()
async def message_handler(message: Message) -> None:
    data = json.loads(message.text)

    collection = await get_collection()

    dataset = await collect_dataset(
        collection,
        datetime.fromisoformat(data["dt_from"]),
        datetime.fromisoformat(data["dt_upto"]),
        data["group_type"]
    )

    await bot.send_message(message.chat.id, json.dumps(dataset))


async def run() -> None:
    await dp.start_polling(bot)
