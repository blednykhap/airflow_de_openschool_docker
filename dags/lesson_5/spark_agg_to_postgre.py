from datetime import datetime, timedelta
from airflow.operators.bash import BashOperator
from airflow import DAG
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator

from pathlib import Path
from airflow.models import Variable
from airflow.decorators import dag, task

with DAG(
        dag_id='spark_agg_data',
        schedule_interval='0 20 * * *',
        start_date=datetime(2023, 9, 5),
        catchup=True,
        dagrun_timeout=timedelta(minutes=60),
        tags=["lesson_5"]
) as dag:
    jars_path = 'jars/postgresql-42.3.1.jar'
    url = f"postgresql://postgres:5432/postgres"


    @task
    def get_regions_to_aggregate(**kwargs):
        regions_to_aggretate = []
        p = Path('data/regions_random')

        regions = [f for f in p.iterdir() if f.is_dir()]
        print(f"regions={regions}")
        for r in regions:
            filepath = Path("/opt/airflow/").joinpath(r).joinpath(f"{kwargs['ds']}.csv")
            print(filepath)
            print(filepath.is_file())
            if filepath.is_file():
                print("appended")
                regions_to_aggretate.append(r)

        return [str(p).split("/")[2] for p in regions_to_aggretate]


    table_regions_path = Variable.get("table_regions_path")

    regions_list = get_regions_to_aggregate()


    @task
    def get_csv_files(regions, **kwargs):
        from textwrap import dedent
        return_list = []

        for r in regions:
            agg_file_path = f'/opt/airflow/data/spark_agg/{r}/{kwargs["ds"]}.csv'
            Path(agg_file_path).parent.mkdir(parents=True, exist_ok=True)
            return_list.append([f'/opt/airflow/data/regions_random/{r}/{kwargs["ds"]}.csv',
                                agg_file_path])
        return return_list


    csv_files = get_csv_files(regions_list)

    spark_read_task = SparkSubmitOperator.partial(
        task_id='spark_read_task',
        conn_id='spark_local',
        application=f'/opt/airflow/dags/lesson_5/spark_scripts/csv2parquet.py',
        name='spark_read_task_app',
        execution_timeout=timedelta(minutes=20)
    ).expand(application_args=csv_files)
