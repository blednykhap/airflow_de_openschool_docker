from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.dummy import DummyOperator
from datetime import datetime


def print_kwargs(**kwargs):
    print("kwargs: ")
    for k, v in kwargs.items():
        print(f"key: {k}, value: {v}")


with DAG(dag_id="lesson_1_print_kwargs_v1",
         start_date=datetime(2023, 9, 5),
         schedule="@once",
         tags=["lesson_1"]) as dag:

    dummy = DummyOperator(task_id="dummy")

    task = PythonOperator(
        task_id="print_kwargs",
        python_callable=print_kwargs,
        provide_context=True
    )

