import os
import findspark
from pyspark.sql import SparkSession
import pyspark
import multiprocessing
import configparser
import boto3

if __name__ == '__main__':
    os.environ['PYSPARK_SUBMIT_ARGS'] = "--packages=org.apache.hadoop:hadoop-aws:2.7.3 pyspark-shell"
    findspark.init()

    config = configparser.ConfigParser()
    config.read(os.path.expanduser("~/.aws/credentials"))
    access_id = config.get('default', "aws_access_key_id") 
    access_key = config.get('default', "aws_secret_access_key")
    

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

    sc=spark.sparkContext
    hadoop_conf=sc._jsc.hadoopConfiguration()
    hadoop_conf.set("fs.s3n.impl", "org.apache.hadoop.fs.s3native.NativeS3FileSystem")
    hadoop_conf.set("fs.s3n.awsAccessKeyId", access_id)
    hadoop_conf.set("fs.s3n.awsSecretAccessKey", access_key)

    s3 = boto3.resource('s3')
    bucket = s3.Bucket('s3-and-boto3')

    for file in bucket.objects.filter(Prefix='pinterest_data_pipeline'):
        df=spark.read.json(f"s3n://s3-and-boto3/{file.key}")
        df.show()

    spark.stop()
