import os

from fastapi import FastAPI, HTTPException
from pymongo import MongoClient
from bson.objectid import ObjectId
from loging_etl import setup_logger


logger = setup_logger()
app = FastAPI()

MONGO_HOST = os.getenv("MONGO_HOST", "mongodb")
MONGO_PORT = int(os.getenv("MONGO_PORT", 27017))
MONGO_DB = os.getenv("MONGO_DB", "airflow")
MONGO_COLLECTION = os.getenv("MONGO_COLLECTION", "nasa_data")

client = MongoClient(host=MONGO_HOST, port=MONGO_PORT, username="airflow", password="airflow")
db = client[MONGO_DB]
collection = db[MONGO_COLLECTION]

@app.get("/")
def root():
    """Корневой endpoint - проверка работы API"""
    logger.debug("Обращение к корневому эндпоинту")
    return {"message": "MongoDB API is working."}

@app.get("/records")
def get_all_records():
    try:
        """Получение всех записей из коллекции"""
        logger.debug("Запрос на получение всех записей из MongoDB")

        records = []
        for doc in collection.find():
            doc["_id"] = str(doc["_id"])
            records.append(doc)

        logger.debug(f"Успешно получено {len(records)} записей")
        return {"records": records}
    except Exception as error:
        logger.error(f"Ошибка при получении всех записей: {error}")
        raise HTTPException(status_code=500, detail="Внутренняя ошибка сервера")


@app.get("/records/{record_id}")
def get_record_by_id(record_id: str):
    try:
        """Получение конкретной записи по ID"""
        logger.debug(f"Запрос на получение записи по ID: {record_id}")

        obj_id = ObjectId(record_id)
        record = collection.find_one({"_id": obj_id})
        if not record:
            raise HTTPException(status_code=404, detail="Record not found")

        record = dict(record)
        record["_id"] = str(record["_id"])

        logger.debug(f"Запись {record_id} успешно получена")

        return record
    except Exception as error:
        logger.error(f"Ошибка при получении записи {record_id}: {error}")
        raise HTTPException(status_code=400, detail="Invalid ObjectId")
