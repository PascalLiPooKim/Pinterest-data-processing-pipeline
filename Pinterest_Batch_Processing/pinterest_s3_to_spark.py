import boto3
import pyspark
import findspark
import multiprocessing
import json
from pyspark.sql import SQLContext
import os
from pyspark.sql.types import StructType, StructField, StringType


if __name__ == '__main__':
    s3 = boto3.resource('s3')
    bucket = s3.Bucket('s3-and-boto3')

    for file in bucket.objects.filter(Prefix='pinterest_data_pipeline'):
        print(file.key)

    s3 = boto3.client('s3')

    # Of course, change the names of the files to match your own.
    

    findspark.init()
    cfg = (
        pyspark.SparkConf()
        # Setting where master node is located [cores for multiprocessing]
        .setMaster(f"local[{multiprocessing.cpu_count()}]")
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


    spark = pyspark.sql.SparkSession.builder.config(conf=cfg).getOrCreate()


    sqlContext = SQLContext(spark)
    # Read JSON file into dataframe
    # df = spark.read.format('org.apache.spark.sql.json') \
    #         .load('pinterest_data_temp.json')

    # df.show()
    if not os.path.exists('pinterest_data_pipeline'):
        os.makedirs('pinterest_data_pipeline')

    
    # s3.download_file('s3-and-boto3', 'pinterest_data_pipeline/pinterest_data_0.json', 
    #     'pinterest_data_0.json')
    # url = 
    # spark.sparkContext.addFile(url)

    # df = sqlContext.read.option("multiline", "true").json('pinterest_data_0.json')
    # os.remove('pinterest_data_0.json')

    # df = spark.createDataFrame([], df.schema)
    # df = df.persist()
    for file in bucket.objects.filter(Prefix='pinterest_data_pipeline'):
        s3.download_file('s3-and-boto3', file.key, 
        file.key)
        # df = df.union(sqlContext.read.option("multiline", "true").json(file.key))
        # os.remove(file.key)

    # df.createOrReplaceTempView("table")
    # spark.sql("REFRESH TABLE table")

    # df_copy = df
    df = sqlContext.read.option("multiline", "true").json('pinterest_data_pipeline/*.json')
    # schema = df.schema
    # print (schema)
    df.printSchema()
    # df.show(False)
    # df = sqlContext.read.schema(schema).json('pinterest_data_temp.json')
    # os.remove('pinterest_data_temp.json')
    # df.show()

    df.select('category', 'title', 'unique_id').show()

    spark.stop()
