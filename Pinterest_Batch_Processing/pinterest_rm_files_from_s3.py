import boto3
import configparser
import os

def del_s3_objects(bucket_name, folder_name):
    s3 = boto3.resource('s3')
    bucket = s3.Bucket(bucket_name)
    client = boto3.client('s3')
    
    # Read the file that contain the name of the JSON files to be deleted
    with open("to_delete.txt", 'r') as f:
        lines = f.readlines()
        
    json_names = [lines[i].rstrip('\n') for i in range(len(lines))]

    # If present in the S3 bucket, delete the files
    for i, file in enumerate(bucket.objects.filter(Prefix=folder_name)):
        if file.key in json_names: # If content of file was stored, delete
            print(f'{i}. Deleting {file.key}')
            client.delete_object(Bucket = bucket_name, Key = file.key)


if __name__ == '__main__':
    
    # Read the required fields from the configuration file
    config = configparser.ConfigParser()
    config_path = "~/AiCore/Pinterest-data-processing-pipeline/Pinterest_Batch_Processing/configurations.ini"
    config.read(os.path.expanduser(config_path))
    pin_data_dir = config.get('default', "pin_data_s3_dir")
    bucket_name = config.get('default', "aws_s3_bucket")
    
    # Delete JSON files whose data has already been stored
    del_s3_objects(bucket_name, pin_data_dir)
    