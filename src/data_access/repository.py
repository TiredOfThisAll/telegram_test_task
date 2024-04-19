from motor.motor_asyncio import AsyncIOMotorCollection, AsyncIOMotorCursor
from datetime import datetime


formats = {
    "month": "%Y-%m",
    "week": "%Y-%U",
    "day": "%Y-%m-%d",
    "hour": "%Y-%m-%d-%H",
}


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
