import logging
import asyncio
import sys

from bot import bot


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, stream=sys.stdout)
    asyncio.run(bot.run())
