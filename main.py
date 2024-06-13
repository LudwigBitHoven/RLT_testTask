from pymongo import MongoClient
import asyncio
import time
import datetime

# довольно простая реализация подключения
client = MongoClient("mongodb://localhost:27017/")
db = client.sampleDB
payment_collection = db.sample_collection


class BaseAggregator:
    _logging = True

    async def run(self, input_json):
        """
            основная точка входа, может засекать время выполнения если _logging = True
        """
        await self.validate_json(input_json)
        start = time.time()
        await self._aggregate(input_json)
        end = time.time()
        if BaseAggregator._logging:
            print(f"Finished, took {end - start} seconds")

    async def validate_json(self, input_json):
        # проверка типа
        if not isinstance(input_json, dict):
            raise ValueError("It's not a json")
        # проверка ключей
        if not (
                input_json.get("dt_from") and
                input_json.get("dt_upto") and
                input_json.get("group_type")):
            raise KeyError("Not enough keys")
        # проверка значений
        group_type_values = ["hour", "day", "week", "month", "year"]
        if not input_json["group_type"] in group_type_values:
            raise ValueError("No such group type")
        try:
            datetime.datetime.fromisoformat(input_json["dt_from"])
            datetime.datetime.fromisoformat(input_json["dt_upto"])
        except Exception:
            raise ValueError("Something wrong with dates")
        if BaseAggregator._logging:
            print("Validation completed successfully")


class Aggregator(BaseAggregator):
    async def _aggregate(self, input_json):
        # перевевдем string в datetime
        input_json["dt_from"] = datetime.datetime.fromisoformat(input_json["dt_from"])
        input_json["dt_upto"] = datetime.datetime.fromisoformat(input_json["dt_upto"])
        date_range = await self.generate_date_range(input_json)
        print(date_range)

    async def generate_date_range(self, input_json):
        """
            генерирует даты с заданным в group_type шагом в пределе от dt_from до dt_upto
        """
        group_type = {input_json["group_type"] + "s": 1}
        dt_from = input_json["dt_from"]
        range_dict = {}
        while dt_from <= input_json["dt_upto"]:
            range_dict[dt_from.isoformat()] = 0
            dt_from += datetime.timedelta(**group_type)
        return range_dict

    async def compose_pipeline(self, input_json):
        """
            составление пайплайна для агрегации
        """
        pass

    async def glue_together(self, groups, date_range):
        """
            склеивание дат и результатов агрегации
        """
        pass


test = {
    "dt_from": "2022-02-01T00:00:00",
    "dt_upto": "2022-02-02T00:00:00",
    "group_type": "hour"
}

temp = Aggregator()
asyncio.run(temp.run(test))
