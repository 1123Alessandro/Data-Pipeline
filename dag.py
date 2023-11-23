from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.bash import BashOperator

with DAG(
    'scheduler',
    default_args = {
        'depends_on_past': True,
        'retries': 1,
        'retry_delay': timedelta(minutes=1),
    },
    start_date = datetime(2021, 1, 1),
    schedule = timedelta(minutes=3),
) as dag:
    t1 = BashOperator(
        task_id = 'clean_names',
        bash_command = 'python3 ./scripts/clean_names.py',
    )

    t2 = BashOperator(
        task_id = 'convert_dates',
        depends_on_past = True,
        bash_command = 'python3 ./scripts/convert_dates.py',
    )

    t3 = BashOperator(
        task_id = 'conform_branches_prices',
        depends_on_past = True,
        bash_command = 'python3 ./scripts/conform_branches_prices.py',
    )

    t4 = BashOperator(
        task_id = 'final_cleanup',
        depends_on_past = True,
        bash_command = 'python3 ./scripts/final_cleanup.py',
    )

    t5 = BashOperator(
        task_id = 'ingest',
        depends_on_past = True,
        bash_command = 'python3 ./scripts/db.py',
    )

    t1 >> t2 >> t3 >> t4 >> t5
    print('done')
