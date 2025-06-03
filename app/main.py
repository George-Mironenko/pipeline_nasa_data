import os

from fastapi import FastAPI, HTTPException
from pymongo import MongoClient
from bson.objectid import ObjectId


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
    return {"message": "MongoDB API is working."}

@app.get("/records")
def get_all_records():
    records = []
    for doc in collection.find():
        doc["_id"] = str(doc["_id"])
        records.append(doc)
    return {"records": records}


@app.get("/records/{record_id}")
def get_record_by_id(record_id: str):
    try:
        obj_id = ObjectId(record_id)
        record = collection.find_one({"_id": obj_id})
        if not record:
            raise HTTPException(status_code=404, detail="Record not found")

        record = dict(record)
        record["_id"] = str(record["_id"])

        return record
    except Exception:
        raise HTTPException(status_code=400, detail="Invalid ObjectId")
