import multiprocessing
import pyspark
import os
import findspark



if __name__ == '__main__':

    os.environ['JAVA_HOME'] = '/usr/lib/jvm/java-8-openjdk-amd64'
    # os.environ['PYSPARK_SUBMIT_ARGS'] = "--master mymaster --total-executor 2 --conf spark.driver.extraJavaOptions=-Dhttp.proxyHost=proxy.mycorp.com-Dhttp.proxyPort=1234 -Dhttp.nonProxyHosts=localhost|.mycorp.com|127.0.0.1 -Dhttps.proxyHost=proxy.mycorp.com -Dhttps.proxyPort=1234 -Dhttps.nonProxyHosts=localhost|.mycorp.com|127.0.0.1 pyspark-shell"
    

    spark_version = '3.2.0'
    client_version = '3.0.0'
    # os.environ['PYSPARK_SUBMIT_ARGS'] = '--packages org.apache.spark:spark-sql-kafka-0-10_2.13:{0}, \
    #     org.apache.kafka:kafka-clients:{1}'.format(spark_version, client_version)
    os.environ['SPARK_HOME'] = "/opt/spark/spark-3.2.0-bin-hadoop3.2"

    SPARK_HOME = findspark.find()
    findspark.init(SPARK_HOME)

    cfg = (
        pyspark.SparkConf()
        # Setting where master node is located [cores for multiprocessing]
        .setMaster(f"local[{multiprocessing.cpu_count()}]")
        # Setting application name
        .setAppName("PinterestApp")
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
    # spark = pyspark.sql.SparkSession.builder.config(
    # conf=pyspark.SparkConf()
    # .setMaster(f"local[{multiprocessing.cpu_count()}]")
    # .setAppName("PinterestApp")
    # ).getOrCreate()

    # spark = pyspark.sql.SparkSession.builder.config().getOrCreate()



    df = spark \
    .readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "'localhost:9092'") \
    .option("subscribe", "ApiToKafkaTopic") \
    .load()
    df.selectExpr("CAST(key AS STRING)", "CAST(value AS STRING)")
    df.show()