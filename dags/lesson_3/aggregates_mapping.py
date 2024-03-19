from __future__ import annotations
from datetime import datetime
from airflow import DAG
from airflow.decorators import task
from airflow.models import Variable
from pathlib import Path
import csv
from pathlib import Path



# All subdirectories in the current directory, not recursive.



with DAG(dag_id="lesson_3_aggregate_mapping_v2",
         start_date=datetime(2023, 9, 4),
         tags=["lesson_3"]
         ) as dag:
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

    @task
    def aggregate_daily_data(region, **kwargs):
        print(f"region = {region}")
        filename = kwargs['ti'].xcom_pull(
            dag_id='lesson_2_generate_daily_data_by_region_v1',
            task_ids=f"generate_daily_data_{region}",
            key="return_value")

        print(f"filename = {filename}")
        user_count = {}
        with open(filename, newline='\n') as csvfile:
            reader = csv.reader(csvfile, delimiter=',')

            for row in reader:
                user_id = row[0]
                if user_id in user_count:
                    user_count[user_id] = user_count[user_id] + 1
                else:
                    user_count[user_id] = 1

        return user_count

    @task
    def sum_it(user_count_dict_list):
        user_count_total = {}
        for next_dict in user_count_dict_list:
            keys = set(user_count_total) | set(next_dict)
            user_count_total = {k: user_count_total.get(k, 0) + next_dict.get(k, 0) for k in keys}

        return user_count_total


    table_regions_path = Variable.get("table_regions_path")

    regions_list = get_regions_to_aggregate()

    added_values = aggregate_daily_data.expand(region=regions_list)

    sum_it(added_values)
