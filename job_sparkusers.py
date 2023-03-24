import airflow
from airflow import DAG
from datetime import timedelta
from airflow.operators.bash import BashOperator

scripts_folder='gs://maseradb-jobs'

default_args = {
    'start_date': airflow.utils.dates.days_ago(0),
    'retries': 1,
	  'retry_delay': timedelta(minutes=2)
}
with DAG('dag_sparkusers_oci_v1',
                  default_args=default_args,
                  schedule_interval='*/10 * * * *',
                  catchup=False) as dag:
    task_etl = BashOperator(
        task_id='job_sparkusers_gcp',
        bash_command=f"python {scripts_folder}/sparkusers.py",
        dag=dag)