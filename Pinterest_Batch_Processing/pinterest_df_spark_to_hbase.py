import pyspark
import findspark
import multiprocessing
from pinterest_s3_to_spark import *


def write_df_to_hbase(df):
    catalog = ''.join("""{
    "table":{"namespace":"default", "name":"pinteresttable"},
    "rowkey":"key",
    "columns":{
        "col0":{"cf":"rowkey", "col":"unique_id", "type":"string"},
        "col1":{"cf":"cf", "col":"category", "type":"string"},
        "col2":{"cf":"cf", "col":"title", "type":"string"},
        "col3":{"cf":"cf", "col":"follower_count", "type":"string"}
    }
    }""".split())

    catalog = ''.join("""{
    "table":{"namespace":"default", "name":"pinteresttable"},
    "rowkey":"key",
    "columns":{
        "col0":{"col":"unique_id", "type":"string"},
        "col1":{"cf":"cf", "col":"category", "type":"string"},
        "col2":{"cf":"cf", "col":"title", "type":"string"},
        "col3":{"cf":"cf", "col":"follower_count", "type":"string"}
    }
    }""".split())

    df.write\
    .options(catalog=catalog)\
    .format("org.apache.spark.sql.execution.datasources.hbase")\
    .save()

if __name__ == '__main__':
    spark, df = create_df()
    write_df_to_hbase(df)
    spark.stop()