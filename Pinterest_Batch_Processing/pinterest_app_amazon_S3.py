import boto3
from kafka import KafkaConsumer
import ast
import os
import tempfile
import pathlib
import json

if __name__ == '__main__':
    consumer = KafkaConsumer('ApiToKafkaTopic', auto_offset_reset='earliest', fetch_min_bytes=256)
    s3_client = boto3.client('s3')
    # temp_file = tempfile.TemporaryFile()
    temp_file = pathlib.Path('pinterest_data.json')
    temp_file.touch(exist_ok=True)
    n = 300
    if n == 1:
        count = 1
    elif n > 1:
        count = 0
    
    with open(temp_file.name, 'r+') as outfile:
        
        for msg in consumer:
            # print (msg.value)
            try:
                byte_str = msg.value
                dict_str = byte_str.decode('utf-8')
                # print (dict_str)
                dict_row = repr(ast.literal_eval(dict_str))

                json.dump(json.dumps(dict_row, indent = 4), outfile)
                # temp_file.write(byte_str)
                print(dict_row)
                print(count)
                if count%n == 0 and count != 0:
                    # s3_client.upload_file('pinterest_data.json', 's3-and-boto3', 
                    # f'pinterest_data_pipeline/pinterest_data_{count//n}.json')
                    outfile.truncate(0)
                    outfile.seek(0)
            except Exception as e:
                print(e)
            finally:
                count += 1