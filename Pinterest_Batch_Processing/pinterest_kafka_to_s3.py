import boto3
from kafka import KafkaConsumer
import ast
import os
import tempfile
import pathlib
import json
import configparser


def send_from_kafka_to_s3(consumer, limit=1000, temp_file = None):
    if temp_file: # if a temporary file is specified
        s3_client = boto3.client('s3')
        with open(temp_file.name, 'r+') as outfile:
        
            for i, msg in enumerate(consumer):
                print (msg)
                try:
                    # Take, decode and convert the value into dictionary
                    byte_str = msg.value
                    dict_str = byte_str.decode('utf-8')
                    dict_row = eval(dict_str)
                    
                    # Write to the temporary JSON file
                    json.dump(dict_row, outfile)
                    
                    print(i, dict_row)
                    
                    # Upload a copy of the JSON file to S3
                    s3_client.upload_file(temp_file, 
                    bucket_name, 
                    f'pinterest_data_pipeline/pinterest_data_{i}.json')
                    
                    # Erase content of file
                    outfile.truncate()
                    outfile.seek(0)
   
                except Exception as e:
                    print(e)
                finally: # Upload only a limited number of data
                    if limit:
                        if i == limit:
                            break
    else: # write directly to S3 instead of uploading a JSON file
        s3_resource = boto3.resource('s3')
        for i, msg in enumerate(consumer):
            
            # Take, decode and convert the value into dictionary
            byte_str = msg.value
            dict_str = byte_str.decode('utf-8')
            dict_row = eval(dict_str)
            
            # Create and write to a JSON file in S3 directly
            obj = s3_resource.Object(bucket_name, 
            f'pinterest_data_pipeline_V2/pinterest_data_{i}.json')
            obj.put(Body=bytes(json.dumps(dict_row).encode('utf-8')))
            print(i, dict_row)
            
            if limit:
                if i == limit:
                    break


if __name__ == '__main__':
    
    # Read fields from the configuration file
    config = configparser.ConfigParser()
    config_path = "~/AiCore/Pinterest-data-processing-pipeline/Pinterest_Batch_Processing/configurations.ini"
    config.read(os.path.expanduser(config_path))
    
    kafka_topic = config.get('default', "kafka_topic") 
    temp_json_path = config.get('default', "temp_json_file")
    bucket_name = config.get('default', "aws_s3_bucket")
    
    # Create a consumer instance
    consumer = KafkaConsumer(kafka_topic, 
                            auto_offset_reset='earliest', 
                            fetch_min_bytes=256)
                                             
    # Create a dummy JSON file to store data
    temp_file = pathlib.Path(temp_json_path)
    temp_file.touch(exist_ok=True)

    # Write/Upload data to AWS S3
    send_from_kafka_to_s3(consumer)