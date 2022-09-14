import boto3
import pyspark
import findspark
import multiprocessing
import json
from pyspark.sql import SQLContext, functions
import os
from pyspark.sql.types import StructType, StructField, StringType
import configparser
import sys
import pathlib




def create_df(bucket_name, folder_name, limit = 100):
    
    # Required to get list of items from the specified bucket
    s3 = boto3.resource('s3')
    bucket = s3.Bucket(bucket_name)

    s3 = boto3.client('s3')

    findspark.init()
    
    # Configurations when creating the Spark session
    cfg = (
        pyspark.SparkConf()
        # Setting where master node is located [cores for multiprocessing]
        # .setMaster(f"local[{multiprocessing.cpu_count()}]")
        # Setting application name
        .setAppName("PinterestAppAmazonS3ToSpark")
        # Setting config value via string
        .set("spark.eventLog.enabled", False)
        # Setting environment variables for executors to use
        .setExecutorEnv(pairs=[("VAR3", "value3"), ("VAR4", "value4")])
        # Setting memory if this setting was not set previously
        .setIfMissing("spark.executor.memory", "1g")
    )

    # Getting a single variable
    print(cfg.get("spark.executor.memory"))
    # Listing all of them in string readable format
    print(cfg.toDebugString())

    # Create the Spark session
    spark = pyspark.sql.SparkSession.builder.config(conf=cfg).getOrCreate()
    
    # Create a text file to store name of JSON files to be deleted later
    path_to_file_delete = 'to_delete.txt'
    temp_file = pathlib.Path(path_to_file_delete)
    temp_file.touch(exist_ok=True)
    
    file_to_delete = open(path_to_file_delete, 'w')

    first_time = True
    # Take each JSON file from the specified prefix/folder
    for i, file in enumerate(bucket.objects.filter(Prefix=folder_name)):
        print(file.key)
        
        # Write JSON file name
        file_to_delete.write(file.key)
        file_to_delete.write('\n')
        
        # Take, decode and convert the data in the file into dictionary
        body = file.get()['Body'].read()
        dict_str = body.decode('utf-8')
        dict_data = eval(dict_str)

        if first_time: # Create a table from the first incoming file
            df = spark.createDataFrame([list(dict_data.values())], list(dict_data.keys()))
            first_time = False
        else: # Add new row with new data
            df = df.union(spark.createDataFrame([list(dict_data.values())], list(dict_data.keys())))

        if i == limit:
            break
    df.printSchema()

    # Select only certain columns to check if the data frame entries are correct (for debugging)
    # df = df.select('unique_id', 'category', 'title', 'follower_count').sort('category')
    df.show()
    
    file_to_delete.close()


    return spark, df

if __name__ == '__main__':
    
    config = configparser.ConfigParser()
    config_path = "~/AiCore/Pinterest-data-processing-pipeline/Pinterest_Batch_Processing/configurations.ini"
    config.read(os.path.expanduser(config_path))
    pin_data_dir = config.get('default', "pin_data_s3_dir")
    bucket_name = config.get('default', "aws_s3_bucket")

    spark, _ = create_df(bucket_name, pin_data_dir)
    spark.stop()
