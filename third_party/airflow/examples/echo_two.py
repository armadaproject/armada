import airflow
from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.bash import BashOperator

import pendulum

with DAG(
    dag_id='echo_two',
    start_date=pendulum.datetime(2016, 1, 1, tz="UTC"),
    schedule_interval='@daily',
    catchup=False,
    default_args={'retries': 2},
) as dag:

    op = BashOperator(task_id='dummy', bash_command='echo Hello World!')
    op_2 = BashOperator(task_id='dummy_2', bash_command='echo Hello World! 2')
    print(op.retries)  # 2hello_task >> notify
    op >> op_2
