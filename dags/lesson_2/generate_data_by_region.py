from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
from airflow.operators.dummy import DummyOperator
from datetime import datetime
import random
from airflow.models import Variable
from pathlib import Path

table_regions_path = Variable.get("table_regions_path")
regions_list = Variable.get("regions_list").split(' ')


def generate_daily_data(_table_regions_path, _region, **kwargs):
    filename = f"{_table_regions_path}/{_region}/{kwargs['ds']}.csv"
    Path(filename).parent.mkdir(parents=True, exist_ok=True)
    rows_count = 100000

    with open(filename, "w") as file:
        header = "user_id,call_time\n"
        file.write(header)

        for i in range(rows_count):
            file.write(
                f"{random.randint(1, 100)},{random.randrange(1000)}\n")

    kwargs['ti'].xcom_push(key="key", value="value")
    kwargs['ti'].xcom_push(key="key", value="value_1")

    return filename


with DAG("lesson_2_generate_daily_data_by_region_v1",
         start_date=datetime(2023, 9, 5),
         schedule="@daily",
         concurrency=1,
         tags=["lesson_2"]) as dag:

    dummy_start_op = DummyOperator(task_id="start")
    for region in regions_list:

        generate_daily_data_op = PythonOperator(
            task_id=f"generate_daily_data_{region}",
            python_callable=generate_daily_data,
            op_kwargs={'_table_regions_path': table_regions_path, '_region': region},
            provide_context=True,
            do_xcom_push=True
        )

        dummy_start_op >> generate_daily_data_op
