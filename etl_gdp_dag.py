from airflow import DAG
from airflow.operators import BashOperator
from datetime import datetime, timedelta

seven_days_ago = datetime.combine(datetime.today() - timedelta(7),
                                    datetime.min.time())

default_args = {
    'owner': 'moraes',
    'depends_on_past': False,
    'start_date': seven_days_ago,
    'email': ['moraes@cloudwalk.com'],
    'email_on_failure': True,
    'email_on_retry': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
    }

dag = DAG('simple', default_args=default_args, schedule_interval='@weekly')

etl_gdp_task = BashOperator(
    task_id='etl_gdp',
    bash_command='python /home/app/load_gdp_data.py',
    dag=dag)