import pyspark
import findspark
import multiprocessing
from pinterest_s3_to_spark import *
import os
import happybase

# bash start-dfs.sh start-yarn.sh start-hbase.sh
# bash stop-dfs.sh stop-yarn.sh
def write_df_to_hbase(df):
    connection = happybase.Connection('localhost')
    table = connection.table('pinterest_table')
    for row in df.collect():
        print(row)
        table.put(row["unique_id"].encode('utf-8'), 
                    {b'cf:category': row["category"].encode('utf-8'),
                     b'cf:title': row["title"].encode('utf-8'),
                     b'cf:follower_count': row["follower_count"].encode('utf-8')})

    

if __name__ == '__main__':
    spark, df = create_df()
    write_df_to_hbase(df)
    spark.stop()