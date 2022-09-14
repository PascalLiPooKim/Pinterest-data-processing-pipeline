import multiprocessing
from time import sleep
from unicodedata import category
from xml.dom.minicompat import StringTypes
import pyspark
import os
import findspark
from pyspark.sql.functions import *
from pyspark.sql.types import *
from itertools import chain
import configparser

def selectCol(str, col):
    # Convert string value to dictionary and take the value(s) of 
    # the specified column
    return eval(str)[col]

def splitUDF(col):
    # Create udf to process the incoming data frame
    return udf(lambda x: selectCol(x, col), StringType())

def write_to_postgres(df, batch_id):
    # Get fields from the configuration file
    config = configparser.ConfigParser()
    config_path = "./configurations.ini"
    config.read(os.path.expanduser(config_path))

    dbHost = config.get("pgAdminAuth", "host")
    dbPort = config.get("pgAdminAuth", "port")
    dbName = config.get("pgAdminAuth", "rdb_name")
    dbUser = config.get("pgAdminAuth", "username")
    dbPassword = config.get("pgAdminAuth", "password")
    dbTable = config.get("pgAdminAuth", "table")
    
    # URL to create the connection
    url = "jdbc:postgresql://"+dbHost+":"+dbPort+"/"+dbName
    properties = {
    "driver": "org.postgresql.Driver",
    "user": dbUser,
    "password": dbPassword,
    }

    # Write the data-freame to pgAdmin
    df.write.jdbc(url=url, table=dbTable, mode="append",
                        properties=properties)



if __name__ == '__main__':

    # Required to download the necessary packages for streaming
    try:
        scala_version = '2.12'
        apache_version = '3.2.1'
        os.environ['PYSPARK_SUBMIT_ARGS'] = '--packages org.apache.spark:spark-sql-kafka-0-10_{0}:{1}, \
            pinterest_app_spark_streaming_to_rdb.py pyspark-shell'.format(scala_version, apache_version)
    except:
        print("Packages not required or already downloaded")

    # Find location of SPARK_HOME
    SPARK_HOME = findspark.find()
    findspark.init(SPARK_HOME)
    print(SPARK_HOME)

    # Create a Spark session with the required configurations
    spark = pyspark.sql.SparkSession.builder \
    .appName("PinterestAppStreaming") \
    .config("spark.jars", "/home/aicore/spark/spark-3.2.1-bin-hadoop3.2/jars/postgresql-42.3.3.jar") \
    .getOrCreate()

    spark.sparkContext.setLogLevel("ERROR")

    # Read the fata from Kafka in real-tile
    df = spark \
    .readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "localhost:9092") \
    .option("subscribe", "ApiToKafkaTopic") \
    .option("startingOffsets", "earliest") \
    .load()
    
    # COnvert the key and value to string
    df.selectExpr("CAST(key AS STRING)", "CAST(value AS STRING)")
    
    df.printSchema()
    
    # Create a new column with contain a string version of the binary value
    df = df.withColumn("converted_value", 
    col('value').cast("string"))
    
    # Only the Pinterest data is needed
    df = df.select("converted_value")
    
    # Columns to be created
    cols = [
        'is_image_or_video', 
        'save_location',
        'unique_id',
        'tag_list',
        'description',
        'index',
        'title',
        'category',
        'downloaded',
        'image_src',
        'follower_count']

    # For each batch create new columns from the converted binary value
    for i in range(len(cols)):
        df = df.withColumn(cols[i], splitUDF(cols[i])(col('converted_value')))
      
    # Select only certain columns to check if the the data frame contains the
    # appropriate data (for debugging)
    # df = df.select('index', 'unique_id', 'category', 'title', 'description')
    df.printSchema()
    
    print('Working')
      
    # Display the data-frame in the console 
    # query = df.writeStream.format("console").option("truncate", 'true').start()

    # Write and append data to postgres database
    query = df.writeStream \
    .outputMode("append") \
    .foreachBatch(write_to_postgres) \
    .option("truncate", 'true') \
    .start()
    
    # Required to perform the query
    query.awaitTermination()