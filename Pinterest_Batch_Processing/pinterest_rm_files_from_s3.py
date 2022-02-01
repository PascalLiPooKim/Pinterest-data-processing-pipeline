import boto3

def del_s3_objects(bucket_name='s3-and-boto3', folder_name='pinterest_data_pipeline'):
    s3 = boto3.resource('s3')
    bucket = s3.Bucket(bucket_name)
    client = boto3.client('s3')

    for file in bucket.objects.filter(Prefix=folder_name):
        print(f'Deleting {file.key} .....')
        client.delete_object(Bucket = bucket_name, Key = file.key)


if __name__ == '__main__':
    del_s3_objects()