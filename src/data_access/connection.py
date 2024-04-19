from motor.motor_asyncio import AsyncIOMotorClient


async def get_collection():
    engine = AsyncIOMotorClient("localhost", 27017)
    db = engine.get_database("sampleDB")
    collection = db.get_collection("sample_collection")
    return collection
