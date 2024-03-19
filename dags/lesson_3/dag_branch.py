from datetime import datetime

from airflow import DAG
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import BranchPythonOperator


def make_decision(**kwargs):
    ed = kwargs['execution_date']
    task_ids = []
    if ed.day % 2 == 0:
        task_ids.append('download_master')
    else:
        task_ids.append('download_visa')
    if ed.day % 3 == 0:
        task_ids.append('download_mir')
    return task_ids


with DAG(dag_id='lesson_3_branching_dag',
         start_date=datetime(2023, 9, 5),
         tags=["lesson_3"]) as dag:
    branch_task = BranchPythonOperator(task_id='make_decision', python_callable=make_decision, provide_context=True)

    task_dm = EmptyOperator(task_id='download_master')
    task_dv = EmptyOperator(task_id='download_visa')
    task_dmir = EmptyOperator(task_id='download_mir')

    task_save_postgres = EmptyOperator(
        task_id='save_postgres'
    )

    branch_task >> task_dv
    branch_task >> task_dm
    branch_task >> task_dmir
    task_dv >> task_save_postgres
    task_dm >> task_save_postgres
