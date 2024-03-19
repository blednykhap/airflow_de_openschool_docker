from datetime import datetime

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import BranchPythonOperator
import sys
from airflow.models import Variable
sys.path.insert(0, "/opt/airflow/dags")

from lesson_2.generate_data_by_region import generate_daily_data


def make_decision(**kwargs):
    ed = kwargs['execution_date']
    task_ids = ['generate_daily_data_MSK',
                'generate_daily_data_SPB',
                'generate_daily_data_KHB']
    if ed.day % 2 == 0:
        task_ids.append('generate_daily_data_POV')
    else:
        task_ids.append('generate_daily_data_SIB')
    if ed.day % 3 == 0:
        task_ids.append('generate_daily_data_SOU')

    return task_ids


with DAG(dag_id='lesson_3_generate_random',
         start_date=datetime(2023, 9, 5),
         tags=["lesson_3"]) as dag:
    branch_task = BranchPythonOperator(
        task_id='make_decision',
        python_callable=make_decision,
        provide_context=True,

    )

    regions_list = Variable.get("regions_list").split(' ')

    for region in regions_list:
        generate_daily_data_op = PythonOperator(
            task_id=f"generate_daily_data_{region}",
            python_callable=generate_daily_data,
            op_kwargs={'_table_regions_path': "data/regions_random", '_region': region},
            provide_context=True,
            do_xcom_push=True
        )

        branch_task >> generate_daily_data_op

