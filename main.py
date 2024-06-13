import asyncio
from aiogram import Bot, Dispatcher
import routes


# Запуск бота
async def main():
    bot = Bot(token="")
    dp = Dispatcher()

    dp.include_routers(routes.router)
    await bot.delete_webhook(drop_pending_updates=True)
    await dp.start_polling(bot)


if __name__ == "__main__":
    asyncio.run(main())
