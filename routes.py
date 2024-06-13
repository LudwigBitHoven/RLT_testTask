from aiogram import Router, F
from aggregator import SumAggregator
from aiogram.types import Message
import json


router = Router()


@router.message(F.text)
async def start(message: Message):
    try:
        input_json = json.loads(message.text)
        aggregator = SumAggregator()
        result = await aggregator.run(input_json)
        await message.reply(f"{result}")
    except Exception:
        await message.reply("Неверный формат")