from pydoc import cli
from sys import api_version
from fastapi import FastAPI
from pydantic import BaseModel, Json
import uvicorn
from json import dumps
from kafka import KafkaProducer

app = FastAPI()

producer = producer = KafkaProducer(bootstrap_servers=['localhost:9092'],
              value_serializer=lambda x: dumps(x).encode('utf-8'), api_version = (0,10,1))

class Data(BaseModel):
    category: str
    index : int
    unique_id : str
    title: str
    description : str
    follower_count : str
    tag_list : str
    is_image_or_video : str
    image_src : str
    downloaded : int
    save_location : str


@app.post("/pin/")
def get_db_row(item: Data):
    data = dict(item)
    producer.send('ApiToKafkaTopic', value=data)
    return 

if __name__ == '__main__':
    uvicorn.run("project_pin_API:app", host="0.0.0.0", port=8000)