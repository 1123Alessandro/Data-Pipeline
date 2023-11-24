from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.bash import BashOperator

dag = DAG(
    'scheduler',
    default_args = {
        'depends_on_past': True,
        'retries': 1,
        'retry_delay': timedelta(minutes=1),
    },
    start_date = datetime(2023, 11, 25),
    schedule = timedelta(minutes=1),
)

t1 = BashOperator(
    task_id = 'clean_names',
    bash_command = 'python3 "/mnt/c/Users/araza/Documents/1/git repos/Data-Pipeline/scripts/clean_names.py"',
    dag=dag,
)

t2 = BashOperator(
    task_id = 'convert_dates',
    bash_command = 'python3 "/mnt/c/Users/araza/Documents/1/git repos/Data-Pipeline/scripts/convert_dates.py"',
    dag=dag,
)

t3 = BashOperator(
    task_id = 'conform_branches_prices',
    bash_command = 'python3 "/mnt/c/Users/araza/Documents/1/git repos/Data-Pipeline/scripts/conform_branches_prices.py"',
    dag=dag,
)

t4 = BashOperator(
    task_id = 'final_cleanup',
    bash_command = 'python3 "/mnt/c/Users/araza/Documents/1/git repos/Data-Pipeline/scripts/final_cleanup.py"',
    dag=dag,
)

t5 = BashOperator(
    task_id = 'ingest',
    bash_command = 'python3 "/mnt/c/Users/araza/Documents/1/git repos/Data-Pipeline/scripts/db.py"',
    dag=dag,
)

t1 >> t2 >> t3 >> t4 >> t5
