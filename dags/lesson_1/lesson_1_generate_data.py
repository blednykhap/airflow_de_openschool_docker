from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
from datetime import datetime
import random


def generate_daily_data(**kwargs):
    filename = f"data/{kwargs['ds']}.csv"
    rows_count = 100000
    regions = ['MSK', 'SPB', 'KHB', 'POV', 'SIB', 'SOU']
    regions_weighs = [50, 30, 10, 4, 3, 3]

    with open(filename, "w") as file:
        header = "user_id,call_region,call_time\n"
        file.write(header)

        for i in range(rows_count):
            file.write(
                f"{random.randint(1, 100)},{random.choices(regions, weights=regions_weighs)[0]},{random.randrange(1000)}\n")

    kwargs['ti'].xcom_push(key="key", value="value")
    kwargs['ti'].xcom_push(key="key", value="value_1")

    return filename


with DAG("lesson_1_generate_daily_data_v1",
         start_date=datetime(2023, 9, 5),
         schedule="@daily",
         concurrency=1,
         tags=["lesson_1"]) as dag:

    generate_daily_data = PythonOperator(
        task_id="generate_daily_data",
        python_callable=generate_daily_data,
        provide_context=True,
        do_xcom_push=True
    )

    get_daily_data_size = BashOperator(
        task_id="get_file_size",
        bash_command="wc -c /opt/airflow/{{ ti.xcom_pull(task_ids='generate_daily_data', key='return_value') }}  | awk '{print $1}'",
        do_xcom_push=True
    )

    generate_daily_data >> get_daily_data_size
