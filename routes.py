from aiogram import Router, F
from aggregator import SumAggregator
from aiogram.types import Message
import json


router = Router()


@router.message(F.text)
async def start(message: Message):
    try:
        # проверяем можно ли создать json из строки
        input_json = json.loads(message.text)
        aggregator = SumAggregator()
        result = await aggregator.run(input_json)
        # заменяем кавычки, возможно это важно при валидации у бота
        result = f"{result}".replace("'", "\"")
        await message.answer(f"{result}")
    except Exception:
        # при возникновении ошибки во время создания json возвращаем пример сообщения
        temp = {"dt_from": "2022-09-01T00:00:00",
                "dt_upto": "2022-12-31T23:59:00", "group_type": "month"}
        await message.answer(f"Невалидный запос. Пример запроса: {temp}")
