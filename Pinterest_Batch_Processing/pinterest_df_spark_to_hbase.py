import pyspark
import findspark
import multiprocessing
from pinterest_s3_to_spark import *
import os


def write_df_to_hbase(df):
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

    # df.write\
    # .options(catalog=catalog)\
    # .format("org.apache.spark.sql.execution.datasources.hbase")\
    # .save()
    df.write\
    .options(catalog=catalog)\
    .option("hbase.spark.use.hbasecontext", False)\
    .option("hbase.table", "pinterest_table")\
    .option('hbase.config.resources', 'file:////home/aicore/hbase-1.7.1/conf/hbase-site.xml') \
    .format("org.apache.spark.sql.execution.datasources.hbase")\
    .save()

    # .format("org.apache.hadoop.hbase.spark")\

    

if __name__ == '__main__':
    os.environ['PYSPARK_SUBMIT_ARGS'] = '--packages com.hortonworks:shc-core:1.1.1-2.1-s_2.11 --repositories http://repo.hortonworks.com/content/groups/public/'
    spark, df = create_df()
    write_df_to_hbase(df)
    spark.stop()