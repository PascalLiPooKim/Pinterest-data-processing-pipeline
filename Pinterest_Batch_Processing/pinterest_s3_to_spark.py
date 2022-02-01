import boto3
import pyspark
import findspark
import multiprocessing
import json
from pyspark.sql import SQLContext, functions
import os
from pyspark.sql.types import StructType, StructField, StringType


def create_df(bucket_name='s3-and-boto3', folder_name='pinterest_data_pipeline'):
    s3 = boto3.resource('s3')
    bucket = s3.Bucket(bucket_name)

    for file in bucket.objects.filter(Prefix=folder_name):
        print(file.key)

    s3 = boto3.client('s3')

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

    first_time = True
    for file in bucket.objects.filter(Prefix='pinterest_data_pipeline'):
        
        body = file.get()['Body'].read()
        dict_str = body.decode('utf-8')
        dict_data = eval(dict_str)
        print(dict_data)

        if first_time:
            df = spark.createDataFrame([list(dict_data.values())], list(dict_data.keys()))
            first_time = False
        else:
            df = df.union(spark.createDataFrame([list(dict_data.values())], list(dict_data.keys())))

    df.printSchema()

    df = df.select('unique_id', 'category', 'title', 'follower_count').sort('category')
    df.show()

    return spark, df

if __name__ == '__main__':
    # s3 = boto3.resource('s3')
    # bucket = s3.Bucket('s3-and-boto3')

    # for file in bucket.objects.filter(Prefix='pinterest_data_pipeline'):
    #     print(file.key)

    # s3 = boto3.client('s3')

    # # Of course, change the names of the files to match your own.
    

    # findspark.init()
    # cfg = (
    #     pyspark.SparkConf()
    #     # Setting where master node is located [cores for multiprocessing]
    #     .setMaster(f"local[{multiprocessing.cpu_count()}]")
    #     # Setting application name
    #     .setAppName("PinterestAppAmazonS3ToSpark")
    #     # Setting config value via string
    #     .set("spark.eventLog.enabled", False)
    #     # Setting environment variables for executors to use
    #     .setExecutorEnv(pairs=[("VAR3", "value3"), ("VAR4", "value4")])
    #     # Setting memory if this setting was not set previously
    #     .setIfMissing("spark.executor.memory", "1g")
    # )

    # # Getting a single variable
    # print(cfg.get("spark.executor.memory"))
    # # Listing all of them in string readable format
    # print(cfg.toDebugString())


    # spark = pyspark.sql.SparkSession.builder.config(conf=cfg).getOrCreate()


    # sqlContext = SQLContext(spark)
    # # Read JSON file into dataframe
    # # df = spark.read.format('org.apache.spark.sql.json') \
    # #         .load('pinterest_data_temp.json')

    # # df.show()
    # # if not os.path.exists('pinterest_data_pipeline'):
    # #     os.makedirs('pinterest_data_pipeline')

    
    # # s3.download_file('s3-and-boto3', 'pinterest_data_pipeline/pinterest_data_0.json', 
    # #     'pinterest_data_0.json')
    # # url = 
    # # spark.sparkContext.addFile(url)

    # # df = sqlContext.read.option("multiline", "true").json('pinterest_data_0.json')
    # # os.remove('pinterest_data_0.json')
    # # schema = ['category', 'index', 'unique_id', 'title', 'description', 
    # #         'follower_count', 'tag_list', 'is_image_or_video', 'image_src', 
    # #         'downloaded', 'save_location']
    # # df = spark.createDataFrame([], schema=schema)
    # # df = df.persist()
    # first_time = True
    # for file in bucket.objects.filter(Prefix='pinterest_data_pipeline'):
    #     # s3.download_file('s3-and-boto3', file.key, 
    #     # file.key)
    #     # response = s3.get_object(Bucket='s3-and-boto3', Key=file.key)
    #     # df = df.union(sqlContext.read.option("multiline", "true").json(file.key))
    #     # os.remove(file.key)
    #     # print(response['Body'])
    #     body = file.get()['Body'].read()
    #     dict_str = body.decode('utf-8')
    #     dict_data = eval(dict_str)
    #     print(dict_data)

    #     if first_time:
    #         df = spark.createDataFrame([list(dict_data.values())], list(dict_data.keys()))
    #         first_time = False
    #     else:
    #         df = df.union(spark.createDataFrame([list(dict_data.values())], list(dict_data.keys())))


    # # df.createOrReplaceTempView("table")
    # # spark.sql("REFRESH TABLE table")
    # # # df_copy = df
    # # df = sqlContext.read.option("multiline", "true").json('pinterest_data_pipeline/*.json')
    # # # schema = df.schema
    # # # print (schema)
    # df.printSchema()
    # # # df.show(False)
    # # # df = sqlContext.read.schema(schema).json('pinterest_data_temp.json')
    # # # os.remove('pinterest_data_temp.json')
    # # df.show()

    # df = df.select('unique_id', 'category', 'title', 'follower_count').sort('category')
    # df.show()


    # catalog = ''.join("""{
    # "table":{"namespace":"default", "name":"pinteresttable"},
    # "rowkey":"key",
    # "columns":{
    #     "col0":{"cf":"rowkey", "col":"unique_id", "type":"string"},
    #     "col1":{"cf":"cf", "col":"category", "type":"string"},
    #     "col2":{"cf":"cf", "col":"title", "type":"string"},
    #     "col3":{"cf":"cf", "col":"follower_count", "type":"string"}
    # }
    # }""".split())

    # catalog = ''.join("""{
    # "table":{"namespace":"default", "name":"pinteresttable"},
    # "rowkey":"key",
    # "columns":{
    #     "col0":{"col":"unique_id", "type":"string"},
    #     "col1":{"cf":"cf", "col":"category", "type":"string"},
    #     "col2":{"cf":"cf", "col":"title", "type":"string"},
    #     "col3":{"cf":"cf", "col":"follower_count", "type":"string"}
    # }
    # }""".split())

    # df.write\
    # .options(catalog=catalog)\
    # .format("org.apache.spark.sql.execution.datasources.hbase")\
    # .save()

    # # spark-submit --packages com.hortonworks:shc:1.0.0-1.6-s_2.10 --repositories http://repo.hortonworks.com/content/groups/public/ --files /home/aicore/hbase-2.4.9/conf/hbase-site.xml pinterest_s3_to_spark.py

    # spark.stop()

    spark, _ = create_df()
    spark.stop()
