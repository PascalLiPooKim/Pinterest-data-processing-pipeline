from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from random import uniform
from datetime import datetime, timedelta


if __name__ == "__main__":
    default_args = {
    'owner': 'Pascal',
    'depends_on_past': False,
    'email': ['paswei98@gmail.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'start_date': datetime(2020, 1, 1), # If you set a datetime previous to the curernt date, it will try to backfill
    'retry_delay': timedelta(minutes=5),
    'end_date': datetime(2022, 1, 1)
    }

    with DAG(dag_id='spark_scheduler_dag', 
    default_args=default_args, 
    schedule_interval='@daily', 
    catchup=False,
    tags=['pyspark_scheduler']) as dag:

        # bash_command = 'spark-submit --packages com.hortonworks:shc:1.0.0-1.6-s_2.10 \
        # --repositories http://repo.hortonworks.com/content/groups/public/ \
        # --files /home/aicore/hbase-2.4.9/conf/hbase-site.xml \
        # /mnt/d/AiCore-perso/Projects/Pinterest-data-processing-pipeline/Pinterest_Batch_Processing/pinterest_s3_to_spark.py'

        hbase_site = '/home/aicore/hbase-1.7.1/conf/hbase-site.xml'


        create_spark_df = '/home/aicore/AiCore/Pinterest-data-processing-pipeline/Pinterest_Batch_Processing/pinterest_s3_to_spark.py'  
        df_create_command = f'spark-submit --packages com.hortonworks:shc:1.0.0-1.6-s_2.10 \
        --repositories http://repo.hortonworks.com/content/groups/public/ \
        --files {hbase_site} {create_spark_df}'
        cretae_df_task = BashOperator(
        task_id='create_spark_df',
        bash_command=df_create_command,
        dag=dag)

        write_df_to_hbase = '/home/aicore/AiCore/Pinterest-data-processing-pipeline/Pinterest_Batch_Processing/pinterest_df_spark_to_hbase.py'
        store_df_command = f'spark-submit --packages com.hortonworks:shc:1.0.0-1.6-s_2.10 \
        --repositories http://repo.hortonworks.com/content/groups/public/ \
        --files {hbase_site} {write_df_to_hbase}'
        write_to_hbase_task = BashOperator(
        task_id='create_spark_df',
        bash_command=store_df_command,
        dag=dag)

        delete_s3_files = '/home/aicore/AiCore/Pinterest-data-processing-pipeline/Pinterest_Batch_Processing/pinterest_rm_files_from_s3.py'
        delete_files_command = f'python3 {delete_s3_files}'
        delete_s3_files_task = BashOperator(
        task_id='delete_s3_objects',
        bash_command=delete_files_command,
        dag=dag)
        # choose_model = PythonOperator(
        # task_id='choose_model',
        # python_callable=choose_best_model
        # )

        create_spark_df >> write_df_to_hbase >> delete_s3_files