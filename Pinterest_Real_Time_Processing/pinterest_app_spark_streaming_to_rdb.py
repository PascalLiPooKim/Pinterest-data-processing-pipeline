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
    return eval(str)[col]

def splitUDF(col):
    return udf(lambda x: selectCol(x, col), StringType())

def write_to_postgres(df, batch_id):
    config = configparser.ConfigParser()
    config_path = "./configurations.ini"
    config.read(os.path.expanduser(config_path))

    dbHost = config.get("pgAdminAuth", "host")
    dbPort = config.get("pgAdminAuth", "port")
    dbName = config.get("pgAdminAuth", "rdb_name")
    dbUser = config.get("pgAdminAuth", "username")
    dbPassword = config.get("pgAdminAuth", "password")
    # dbTable = config.get("pgAdminAuth", "table")
    

    url = "jdbc:postgresql://"+dbHost+":"+dbPort+"/"+dbName
    properties = {
    "driver": "org.postgresql.Driver",
    "user": dbUser,
    "password": dbPassword,
    }

    df.write.jdbc(url=url, table="pinterest", mode="append",
                        properties=properties)



if __name__ == '__main__':

    # os.environ['JAVA_HOME'] = '/usr/lib/jvm/java-8-openjdk-amd64'
    # os.environ['PYSPARK_SUBMIT_ARGS'] = "--master mymaster --total-executor 2 --conf spark.driver.extraJavaOptions=-Dhttp.proxyHost=proxy.mycorp.com-Dhttp.proxyPort=1234 -Dhttp.nonProxyHosts=localhost|.mycorp.com|127.0.0.1 -Dhttps.proxyHost=proxy.mycorp.com -Dhttps.proxyPort=1234 -Dhttps.nonProxyHosts=localhost|.mycorp.com|127.0.0.1 pyspark-shell"
    

    # spark_version = '3.2.1'
    # client_version = '3.0.0'
    # os.environ['PYSPARK_SUBMIT_ARGS'] = '--packages org.apache.spark:spark-sql-kafka-0-10_2.13:{0}, \
    #     org.apache.kafka:kafka-clients:{1}'.format(spark_version, client_version)

    try:
        scala_version = '2.12'
        apache_version = '3.2.1'
        os.environ['PYSPARK_SUBMIT_ARGS'] = '--packages org.apache.spark:spark-sql-kafka-0-10_{0}:{1}, \
            pinterest_app_spark_streaming_to_rdb.py pyspark-shell'.format(scala_version, apache_version)
    except:
        print("Packages not required or already downloaded")

    # os.environ['PYSPARK_SUBMIT_ARGS']="--master spark://localhost:7077"
    # os.environ['SPARK_HOME'] = "/home/aicore/spark/spark-3.2.1-bin-hadoop3.2"

    SPARK_HOME = findspark.find()
    findspark.init(SPARK_HOME)
    # findspark.init()
    print(SPARK_HOME)
    # sleep(5)

    # cfg = (
    #     pyspark.SparkConf()
    #     # Setting where master node is located [cores for multiprocessing]
    #     .setMaster(f"local[{multiprocessing.cpu_count()}]")
    #     # Setting application name
    #     .setAppName("PinterestApp")
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

    # # spark = pyspark.sql.SparkSession.builder.config(conf=cfg).getOrCreate()
    # spark = pyspark.sql.SparkSession.builder.config(
    # conf=pyspark.SparkConf()
    # .setMaster(f"local[{multiprocessing.cpu_count()}]")
    # .setAppName("PinterestApp")
    # ).getOrCreate()

    spark = pyspark.sql.SparkSession.builder \
    .appName("PinterestAppStreaming") \
    .config("spark.jars", "/home/aicore/spark/spark-3.2.1-bin-hadoop3.2/jars/postgresql-42.3.3.jar") \
    .getOrCreate()

    spark.sparkContext.setLogLevel("ERROR")

    df = spark \
    .readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "localhost:9092") \
    .option("subscribe", "ApiToKafkaTopic") \
    .option("startingOffsets", "earliest") \
    .load()
    
    # spark.stop()

    df.selectExpr("CAST(key AS STRING)", "CAST(value AS STRING)")
    # df.show()

    # schema = StructType().add("a", StringType()).add("b", StringType())
    # df = df.select( \
    # col("key").cast("string"),
    # from_json(col("value").cast("string"), schema))
    df.printSchema()
    # sleep(5)

    # df.show()
    # schema = df.schema
    # print(schema)
    # print(df.value)
    # df.collect()
    # df = df.select("value")
    df = df.withColumn("converted_value", 
    col('value').cast("string"))
    df = df.select("converted_value")
    # df = df.withColumn('category', lit(None).cast(StringType()))
    # df = df.rdd.map(lambda x: (x.converted_value, 
    # x['category'])) \
    # .toDF(["converted_value", 'category'])
    print(df.converted_value)
    # print(split(df.converted_value, ':'))
    # sleep(5)
    # df = df.withColumn('category', create_map([lit(x) for x in chain(*df['converted_value'].items())]))
    # df.printSchema()
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

    # def convertToDict(str, col):
    #     return eval(str)[col]

    # def splitUDF(col):
    #     return udf(lambda x: convertToDict(x, col), StringType())
    
    # schema = StructType([StructField('data', MapType(StringType(), StringType()))])
    # df = df.withColumn('category', from_json(df.converted_value, schema))

    # -----------
    for i in range(len(cols)):
        df = df.withColumn(cols[i], splitUDF(cols[i])(col('converted_value')))
        
    df = df.select('index', 'unique_id', 'category', 'title', 'description')
    # query = df.writeStream.format("console").option("truncate", 'true').start()
    df.printSchema()
    print('Working')
    
    

    # db_name = config.get("pgAdminAuth", "rdb_name")
    

    # def func1(row):
    #     print(row)
    #     return row['converted_value'].split(':')
    
    
    # def write_to_postgres(df, batch_id):
    #     config = configparser.ConfigParser()
    #     config_path = "./configurations.ini"
    #     config.read(os.path.expanduser(config_path))

    #     dbHost = config.get("pgAdminAuth", "host")
    #     dbPort = config.get("pgAdminAuth", "port")
    #     dbName = config.get("pgAdminAuth", "rdb_name")
    #     dbUser = config.get("pgAdminAuth", "username")
    #     dbPassword = config.get("pgAdminAuth", "password")
        

    #     url = "jdbc:postgresql://"+dbHost+":"+dbPort+"/"+dbName
    #     properties = {
    #     "driver": "org.postgresql.Driver",
    #     "user": dbUser,
    #     "password": dbPassword
    #     }

    #     df.write.jdbc(url=url, table="metrics", mode="append",
    #                         properties=properties)


    # def foreach_batch_func():
    #     def _(df, epoch_id):
    #         write_to_postgres(df)
    #     return _

    query = df.writeStream \
    .outputMode("append") \
    .foreachBatch(write_to_postgres) \
    .option("truncate", 'true') \
    .start()
    # query = df.writeStream.foreach(func1).format("console").option("truncate", 'true').start()
    query.awaitTermination()
    # spark.stop()
    # spark-submit --packages org.apache.spark:spark-sql-kafka-0-10_2.13:3.2.1 org.apache.kafka:kafka-clients:3.0.0 pinterest_app_spark_streaming.py