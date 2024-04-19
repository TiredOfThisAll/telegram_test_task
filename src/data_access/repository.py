from motor.motor_asyncio import AsyncIOMotorCollection, AsyncIOMotorCursor
from datetime import datetime

from utils.time import divide_time_period


formats = {
    "month": "%Y-%m",
    "week": "%Y-%U",
    "day": "%Y-%m-%d",
    "hour": "%Y-%m-%d-%H",
}


async def collect_dataset(collection: AsyncIOMotorCollection, start_time: datetime, end_time: datetime, group_type: str) -> dict:
    labels = divide_time_period(start_time, end_time, group_type)
    labels_str = [label.isoformat() for label in labels]
    whole_period_cursor = await aggregate_period(collection, start_time, end_time, group_type)

    data_dict = {}
    async for item in whole_period_cursor:
        data_dict[item["iso_date"]] = item["value"]

    dataset = []
    for label in labels_str:
        if label not in data_dict:
            dataset.append(0)
        else:
            dataset.append(data_dict[label])
    return {"dataset": dataset, "labels": labels_str}



async def aggregate_period(collection: AsyncIOMotorCollection, start_time: datetime, end_time: datetime, group_type) -> AsyncIOMotorCursor:
    """
    This method runs async aggregation
    :return: 
    """

    pipeline = [
    {
        '$match': {
            'dt': {
                '$gte': start_time,
                '$lte': end_time
            }
        }
    },
    {
        '$addFields': {
            "group_type": {
                '$dateToString': {
                    'format': formats[group_type],
                    'date': { '$toDate': "$dt" }
                }
            }
        }
    },
    {
        '$group': {
            '_id': "$group_type",
            'total_value': { '$sum': "$value" }
        }
    },
    {
        '$project': {
            '_id': 0,
            'value': "$total_value",
            'iso_date': {
                '$dateToString': {
                    'format': "%Y-%m-%dT%H:%M:%S",
                    'date': { '$toDate': "$_id" }
                }
            }
        }
    },
    {
        '$sort': {
            "group_type": 1
        }
    }
    ]

    return collection.aggregate(
        pipeline=pipeline,
        allowDiskUse=True
    )
