import boto3
from kafka import KafkaConsumer
import ast
import os
import tempfile
import pathlib
import json
import configparser

# config = {'Topic_name': 'ApiToKafkaTopic', 
#         'Temporary_JSON_file': 'pinterest_data.json',
#         'AWS_S3_bucket_name': 's3-and-boto3'
#         }

def send_from_kafka_to_s3(consumer, temp_file = None):
    if temp_file:
        s3_client = boto3.client('s3')
        with open(temp_file.name, 'r+') as outfile:
        # count = 0
            for i, msg in enumerate(consumer):
                print (msg)
                try:
                    byte_str = msg.value
                    dict_str = byte_str.decode('utf-8')
                    # print (dict_str)
                    # dict_row = repr(ast.literal_eval(dict_str))
                    dict_row = eval(dict_str)
                    # dict_row = msg
                    json.dump(dict_row, outfile)
                    # temp_file.write(byte_str)
                    # print(dict_row)
                    print(i, dict_row)
                    # if count%n == 0 and count != 0:
                    s3_client.upload_file(temp_file, 
                    bucket_name, 
                    f'pinterest_data_pipeline/pinterest_data_{i}.json')
                    outfile.truncate()
                    outfile.seek(0)

                    # count += 1
                except Exception as e:
                    print(e)
                finally:
                    if i == 1000:
                        break
    else:
        s3_resource = boto3.resource('s3')
        for i, msg in enumerate(consumer):
            byte_str = msg.value
            dict_str = byte_str.decode('utf-8')
            dict_row = eval(dict_str)
            obj = s3_resource.Object(bucket_name, 
            f'pinterest_data_pipeline_V2/pinterest_data_{i}.json')
            obj.put(Body=bytes(json.dumps(dict_row).encode('utf-8')))
            print(dict_row)
            if i == 1000:
                break


if __name__ == '__main__':
    # consumer = KafkaConsumer(config['Topic_name'], 
    #                         auto_offset_reset='earliest', 
    #                         fetch_min_bytes=256)
    config = configparser.ConfigParser()
    config_path = "~/AiCore/Pinterest-data-processing-pipeline/Pinterest_Batch_Processing/configurations.ini"
    config.read(os.path.expanduser(config_path))
    kafka_topic = config.get('default', "kafka_topic") 
    temp_json_path = config.get('default', "temp_json_file")
    bucket_name = config.get('default', "aws_s3_bucket")
    
    # consumer = KafkaConsumer(config['Topic_name'], 
    #                         auto_offset_reset='earliest', 
    #                         fetch_min_bytes=256)

    consumer = KafkaConsumer(kafka_topic, 
                            auto_offset_reset='earliest', 
                            fetch_min_bytes=256)
                            
                            
    # s3_client = boto3.client('s3')
    # temp_file = tempfile.TemporaryFile()
    # temp_file = pathlib.Path(config['Temporary_JSON_file'])
    temp_file = pathlib.Path(temp_json_path)
    temp_file.touch(exist_ok=True)
    # n = 1
    # if n == 1:
    #     count = 1
    # elif n > 1:
    #     count = 0
    
    # with open(temp_file.name, 'r+') as outfile:
    #     # count = 0
    #     for i, msg in enumerate(consumer):
    #         print (msg)
    #         try:
    #             byte_str = msg.value
    #             dict_str = byte_str.decode('utf-8')
    #             # print (dict_str)
    #             # dict_row = repr(ast.literal_eval(dict_str))
    #             dict_row = eval(dict_str)
    #             # dict_row = msg
    #             json.dump(dict_row, outfile)
    #             # temp_file.write(byte_str)
    #             # print(dict_row)
    #             print(i, dict_row)
    #             # if count%n == 0 and count != 0:
    #             s3_client.upload_file(temp_file, 
    #             bucket_name, 
    #             f'pinterest_data_pipeline/pinterest_data_{i}.json')
    #             outfile.truncate()
    #             outfile.seek(0)

    #             # count += 1
    #         except Exception as e:
    #             print(e)
    #         finally:
    #             if i == 10:
    #                 break

    
    # s3_resource = boto3.resource('s3')
    # for i, msg in enumerate(consumer):
    #     byte_str = msg.value
    #     dict_str = byte_str.decode('utf-8')
    #     dict_row = eval(dict_str)
    #     obj = s3_resource.Object(bucket_name, f'pinterest_data_pipeline_V2/pinterest_data_{i}.json')
    #     obj.put(bytes(json.dumps(dict_row)))

    send_from_kafka_to_s3(consumer)