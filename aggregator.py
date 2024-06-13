from pymongo import MongoClient
import asyncio
import time
import datetime

# довольно простая реализация подключения
client = MongoClient("mongodb://localhost:27017/")
db = client.sampleDB
payment_collection = db.sample_collection


class BaseAggregator:
    _logging = False

    async def run(self, input_json):
        """
            основная точка входа, может засекать время выполнения, а также
            тречить выполнение валидации, если _logging = True
        """
        await self.validate_json(input_json)
        start = time.time()
        result = await self._aggregate(input_json)
        end = time.time()
        if BaseAggregator._logging:
            print(f"Finished, took {end - start} seconds")
        return result

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


class SumAggregator(BaseAggregator):
    async def _aggregate(self, input_json):
        # перевевдем string в datetime
        input_json["dt_from"] = datetime.datetime.fromisoformat(
            input_json["dt_from"])
        input_json["dt_upto"] = datetime.datetime.fromisoformat(
            input_json["dt_upto"])
        date_range = await self.generate_date_range(input_json)
        pipeline = await self.compose_pipeline(input_json)
        groups = list(payment_collection.aggregate(pipeline))
        return await self.glue_together(groups, date_range)

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
        if BaseAggregator._logging:
            print(f"generate_date_range finished")
        return range_dict

    async def compose_pipeline(self, input_json):
        """
            составление пайплайна для агрегации
        """
        # мэтчим группировку
        format_type = "%Y-%m-%dT00:00:00"
        if input_json["group_type"] == "hour":
            format_type = "%Y-%m-%dT%H:00:00"
        elif input_json["group_type"] == "day":
            input_json["group_type"] = "dayOfYear"
        elif input_json["group_type"] == "month":
            format_type = "%Y-%m-01T00:00:00"
        elif input_json["group_type"] == "year":
            format_type = "%Y-01-01T00:00:00"
        group_type = "$" + input_json["group_type"]
        # строим пайплайн по шаблону, скорее всего есть реализация гораздо проще
        pipeline = [
            {
                "$match": {"dt": {"$gte": input_json["dt_from"], "$lte": input_json["dt_upto"]}},
            },
            {
                "$group": {
                    "_id": {
                        "day": {group_type: "$dt"}
                    },
                    "dataset": {"$sum": "$value"},
                    "date": {"$first": "$dt"}
                }
            },
            {
                "$sort": {"date": 1}
            },
            {
                "$project": {
                    "labels": {"$dateToString": {"format": format_type, "date": "$date"}},
                    "_id": 0,
                    "dataset": 1
                }
            }
        ]
        if BaseAggregator._logging:
            print(f"compose_pipeline finished")
        return pipeline

    async def glue_together(self, groups, date_range):
        """
            функция для склеивания дат из сгенерированного словаря и пайплайна с суммами,
            таким образом учитываются даты с нулевыми суммами
        """
        result = {"dataset": [], "labels": []}
        for record in groups:
            if record["labels"] in date_range:
                date_range[record["labels"]] = record["dataset"]
        for i in date_range:
            result["labels"].append(i)
            result["dataset"].append(date_range[i])
        if BaseAggregator._logging:
            print(f"glue_together finished, result is: {result}")
        return result


if __name__ == "__main__":
    test = {
        "dt_from": "2022-10-01T00:00:00",
        "dt_upto": "2022-11-30T23:59:00",
        "group_type": "day"
    }

    temp = SumAggregator()
    print(asyncio.run(temp.run(test)))
