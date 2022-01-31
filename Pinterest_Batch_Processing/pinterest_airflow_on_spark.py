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

        bash_command = 'spark-submit --packages com.hortonworks:shc:1.0.0-1.6-s_2.10 \
        --repositories http://repo.hortonworks.com/content/groups/public/ \
        --files /home/aicore/hbase-2.4.9/conf/hbase-site.xml \
        /mnt/d/AiCore-perso/Projects/Pinterest-data-processing-pipeline/Pinterest_Batch_Processing/pinterest_s3_to_spark.py'

        run_spark_submit_task = BashOperator(
        task_id='run_spark_submit',
        bash_command=bash_command,
        dag=dag)

        # choose_model = PythonOperator(
        # task_id='choose_model',
        # python_callable=choose_best_model
        # )