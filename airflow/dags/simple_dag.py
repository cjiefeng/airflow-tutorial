import datetime as dt

from airflow import DAG
from airflow.operators.bash_operator import BashOperator
from airflow.operators.python_operator import PythonOperator


def timing():
    now = dt.datetime.now().time()
    minute = now.minute
    return 100 / minute  # fail when 0min


def respond():
    return 'Greet Responded Again'


default_args = {
    'owner': 'airflow',
    'start_date': dt.datetime(2021, 8, 23, 13, 00, 00),
    'concurrency': 1,
    'retries': 0
}

with DAG('simple_dag',
         default_args=default_args,
         schedule_interval='*/30 * * * *',
         ) as dag:
    opr_fail = PythonOperator(task_id='py_fail',
                              python_callable=timing)

    opr_sleep = BashOperator(task_id='sh_sleep',
                             trigger_rule="all_done",
                             bash_command='sleep 5')

    opr_sleep_strict = BashOperator(task_id='sh_sleep_strict',
                                    trigger_rule="all_success",
                                    bash_command='sleep 5')

    opr_hello = BashOperator(task_id='sh_hi',
                             bash_command='echo "Hi!!"')

    opr_respond = PythonOperator(task_id='py_respond',
                                 trigger_rule="all_done",
                                 python_callable=respond)

opr_fail >> [opr_sleep, opr_sleep_strict]
opr_sleep >> opr_respond << opr_hello
opr_sleep_strict >> opr_respond