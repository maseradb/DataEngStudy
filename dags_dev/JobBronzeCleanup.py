from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime, timedelta

import pendulum

local_tz = pendulum.timezone("America/Sao_Paulo")



default_args = {
    #'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2023, 4, 20, tzinfo=local_tz),
    'email': ['airflow@example.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

dag = DAG('Job_Bronze_Cleanup', default_args=default_args, schedule_interval='0 2 * * *', catchup=False)

t1 = BashOperator(
    task_id='BronzeCleanup',
    bash_command='python3 /home/maseradb/DataEngStudy/sql_to_delta_vacuum.py ',
    dag=dag)
