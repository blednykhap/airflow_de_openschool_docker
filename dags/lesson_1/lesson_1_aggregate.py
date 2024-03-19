from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime
import csv


def aggregate_daily_data(**kwargs):
    filename = f"data/{kwargs['ds']}.csv"
    regions_count = {}
    with open(filename, newline='\n') as csvfile:
        reader = csv.reader(csvfile, delimiter=',')

        for row in reader:
            region = row[1]
            if region in regions_count:
                regions_count[region] = regions_count[region] + 1
            else:
                regions_count[region] = 1

    return regions_count


with DAG("lesson_1_aggregate_daily_data",
         start_date=datetime(2023, 9, 5),
         schedule="@daily",
         max_active_runs=1,
         tags=["lesson_1"]
         ) as dag:
    generate_daily_data = PythonOperator(
        task_id="aggregate_daily_data",
        python_callable=aggregate_daily_data,
        provide_context=True,
        do_xcom_push=True,
        depends_on_past=True
    )
