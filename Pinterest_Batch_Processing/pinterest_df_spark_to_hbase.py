import pyspark
import findspark
import multiprocessing
from pinterest_s3_to_spark import *
import os


def write_df_to_hbase(df):

    # Select only certain columns. Comment the line to get all columns
    # (for debugging)
    # df = df.select('unique_id', 'category', 'title', 'follower_count')

    dataSourceFormat = "org.apache.hadoop.hbase.spark"

    # Write to HBase by specifying the required columns and family column.
    # Update the mapping option if more/less columns are selected
    df.write \
    .option("hbase.spark.use.hbasecontext", False)\
    .option("hbase.table", "pinterest_table")\
    .option("hbase.columns.mapping", 
            "unique_id STRING :key , \
            category STRING cf:category, \
            title STRING cf:title, \
            follower_count STRING cf:followers_count, \
            index STRING cf:index, \
            tag_list STRING cf:tag_list, \
            description STRING cf:description, \
            downloaded STRING cf:downloaded, \
            is_image_or_video STRING cf:is_image_or_video, \
            image_src STRING cf:image_src") \
    .option("hbase.spark.pushdown.columnfilter", False) \
    .format(dataSourceFormat) \
    .save()
    
    df.show()
    

if __name__ == '__main__':
    # Read fields from configuration file
    config = configparser.ConfigParser()
    config_path = "~/AiCore/Pinterest-data-processing-pipeline/Pinterest_Batch_Processing/configurations.ini"
    config.read(os.path.expanduser(config_path))
    pin_data_dir = config.get('default', "pin_data_s3_dir")
    bucket_name = config.get('default', "aws_s3_bucket")
    
    # Create data frame from the S3 bucket
    spark, df = create_df(bucket_name, pin_data_dir)
    # Write data frame to HBase
    write_df_to_hbase(df)
    spark.stop()