import pyspark
import findspark
import multiprocessing
from pinterest_s3_to_spark import *
import os


def write_df_to_hbase(df):

    # --master yarn-client --packages com.hortonworks:shc-core:1.1.1-2.1-s_2.11 --repositories http://nexus-private.hortonworks.com/nexus/content/repositories/IN-QA/

    # try:
    #     # os.environ['PYSPARK_SUBMIT_ARGS'] = '--master yarn-client \
    #     #     --packages com.hortonworks:shc:1.1.1-2.1-s_2.11 \
    #     #     --repositories http://repo.hortonworks.com/content/groups/public/ \
    #     #     --files /home/aicore/hbase-1.7.1/conf/hbase-site.xml \
    #     #     pinterest_df_spark_to_hbase.py pyspark-shell'
    #     os.environ['PYSPARK_SUBMIT_ARGS'] = '--packages it.nerdammer.bigdata:spark-hbase-connector_2.10:1.0.3 \
    #         --repositories git@github.com:nerdammer/spark-hbase-connector \
    #         pinterest_df_spark_to_hbase.py pyspark-shell'
    # except:
    #     print('Packages not required')

    df = df.select('unique_id', 'category', 'title', 'follower_count')

    catalog = ''.join("""{
    "table":{"namespace":"default", "name":"pinterest_table"},
    "rowkey":"key",
    "columns":{
        "col0":{"cf":"rowkey", "col":"unique_id", "type":"string"},
        "col1":{"cf":"cf", "col":"category", "type":"string"},
        "col2":{"cf":"cf", "col":"title", "type":"string"},
        "col3":{"cf":"cf", "col":"follower_count", "type":"string"}
    }
    }""".split())

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

    # dataSourceFormat = "org.apache.spark.sql.execution.datasources.hbase"
    dataSourceFormat = "org.apache.hadoop.hbase.spark"

    # df.write\
    # .options(catalog=catalog)\
    # .format(dataSourceFormat)\
    # .save()
    df.write\
    .option("hbase.spark.use.hbasecontext", False)\
    .option("hbase.table", "pinterest_table")\
    .option("hbase.columns.mapping", 
            "unique_id STRING :key , \
            category STRING cf:category, \
            title STRING cf:title, \
            follower_count STRING cf:followers_count") \
    .option("hbase.spark.pushdown.columnfilter", False) \
    .format(dataSourceFormat) \
    .save()
    
    df.show()
    
    # .option('hbase.config.resources', 'file:////home/aicore/hbase-1.7.1/conf/hbase-site.xml') \
    # .format("org.apache.spark.sql.execution.datasources.hbase")\

    # .format("org.apache.hadoop.hbase.spark")\

    

if __name__ == '__main__':
    # os.environ['PYSPARK_SUBMIT_ARGS'] = '--packages com.hortonworks:shc-core:1.1.1-2.1-s_2.11 \
    #     --repositories http://repo.hortonworks.com/content/groups/public/'
    spark, df = create_df()
    write_df_to_hbase(df)
    spark.stop()