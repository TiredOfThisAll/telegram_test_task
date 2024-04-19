from os import getenv
import json

from aiogram import Bot, Dispatcher
from aiogram.client.default import DefaultBotProperties
from aiogram.enums import ParseMode
from aiogram.types import Message



TOKEN = getenv("BOT_TOKEN")

bot = Bot(token=TOKEN, default=DefaultBotProperties(parse_mode=ParseMode.HTML))

dp = Dispatcher()


@dp.message()
async def message_handler(message: Message) -> None:
    data = json.loads(message.text)

    await bot.send_message(message.chat.id, json.dumps(data))


async def run() -> None:
    await dp.start_polling(bot)
