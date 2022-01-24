from pydoc import cli
from sys import api_version
from fastapi import FastAPI
from pydantic import BaseModel, Json
import uvicorn
from json import dumps
# from kafka import KafkaProducer
import kafka

app = FastAPI()


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


producer = kafka.KafkaProducer(bootstrap_servers='localhost:9092', value_serializer=lambda x: 
                         dumps(x).encode('utf-8'), api_version = (0,10,1))
# producer = kafka.KafkaProducer(bootstrap_servers='localhost:9092', api_version = (0,10,1))

@app.post("/pin/")
def get_db_row(item: Data):
    # print(item.__dict__)
    data = item.__dict__
    # data = dumps(data)
    producer.send('ApiToKafkaTopic', data)
    return item

if __name__ == '__main__':
    uvicorn.run("project_pin_API:app", host="0.0.0.0", port=80)