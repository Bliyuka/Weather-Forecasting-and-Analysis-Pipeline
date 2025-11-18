from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.python import PythonOperator


default_args = {
    'owner': 'bliyuka',
    'retries': 5,
    'retry_delay': timedelta(minutes=5)
}

def get_pandas():
    import pandas as pd
    print(f'Pandas version imported: {pd.__version__}')

def get_matplotlib():
    import matplotlib
    print(f'matplotlib version imported: {matplotlib.__version__}')



with DAG(
    default_args=default_args,
    dag_id="python_dag_lib_v02",
    # description="My first dag with python operator",
    start_date=datetime(2025, 9, 20),
    schedule_interval="@daily"
) as dag:
    get_pandas = PythonOperator(
        task_id="get_pandas",
        python_callable=get_pandas,
    )
    
    get_matplotlib = PythonOperator(
        task_id="get_matplotlib",
        python_callable=get_matplotlib,
    )
    
    
    get_pandas >> get_matplotlib
    