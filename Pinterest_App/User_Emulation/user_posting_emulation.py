import requests
from time import sleep
import random
import uvicorn
from multiprocessing import Process
import boto3
import json
import sqlalchemy
from read_aws_credentials_locally import read_in_aws_credentials
import pymysql

random.seed(100)

creds = read_in_aws_credentials()

class AWSDBConnector:

    def __init__(self):

        __client = boto3.client('secretsmanager', region_name="eu-west-1", 
            aws_access_key_id=creds[0], 
            aws_secret_access_key=creds[1]
            )
            
        __response = __client.get_secret_value(
            SecretId = 'arn:aws:secretsmanager:eu-west-1:710573529953:secret:project_pin_credentials-BpVLOt'
        )

        __secrets_dict = json.loads(__response['SecretString'])

        self.HOST = __secrets_dict["host"]
        self.USER = __secrets_dict["username"]
        self.PASSWORD = __secrets_dict["password"]
        self.DATABASE = __secrets_dict["dbname"]
        self.PORT = __secrets_dict["port"]
        
    def create_db_connector(self):
        engine = sqlalchemy.create_engine(f"mysql+pymysql://{self.USER}:{self.PASSWORD}@{self.HOST}:{self.PORT}/{self.DATABASE}?charset=utf8mb4")
        return engine


new_connector = AWSDBConnector()


def run_infinite_post_data_loop():
    while True:
        sleep(random.randrange(0, 2))
        random_row = random.randint(0, 11000)
        engine = new_connector.create_db_connector()
        selected_row = engine.execute(f"SELECT * FROM pinterest_data LIMIT {random_row}, 1")
        # result = dict(selected_row.mappings().all()[0])
        print(selected_row)
        for row in selected_row:
            result = dict(row)
            requests.post("http://localhost:8000/pin/", json=result)
            print(result)


if __name__ == "__main__":
    run_infinite_post_data_loop()
    print('Working')
    
    


