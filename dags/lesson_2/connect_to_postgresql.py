from airflow import DAG
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from datetime import datetime


def save_csv(conn_id, **kwargs):
    ds = kwargs['ds']
    import sys, psycopg2
    hook = PostgresHook(postgres_conn_id=conn_id)
    conn = hook.get_conn()  # this returns psycopg2.connect() object

    # conn = psycopg2.connect("dbname='dvdrental' user='postgres' host='localhost' password='root'")
    cur = conn.cursor()
    print('Connecting to Database')
    sql = f"COPY (select * from dag where next_dagrun > '{ds}') TO STDOUT WITH CSV DELIMITER ','"
    with open("data/dags.csv", "w") as file:
        cur.copy_expert(sql, file)
        cur.close()
    print('CSV File has been created')


with DAG(dag_id="lesson_2_connect_to_airflow_postgresql_v2",
         start_date=datetime(2023, 9, 5),
         schedule="@once",
         tags=["lesson_2"]) as dag:
    test_connection = PostgresOperator(
        task_id="connect_to_airflow_postgresql",
        sql="select * from dag where next_dagrun > '{{ ds }}'",
        postgres_conn_id="airflow_postgresql",
        do_xcom_push=True
    )

    save_csv_task = PythonOperator(
        task_id="save_csv",
        python_callable=save_csv,
        provide_context=True,
        op_kwargs={"conn_id": "airflow_postgresql"}
    )

    test_connection >> save_csv_task
