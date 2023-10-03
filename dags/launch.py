# Start from 60 days ago and future
# Is a rocket launch scheduled for today? -> get rocket launches scheduled for today
# MAYBE: rockets ids, name, mission names, launch statuses, country information, launch service providers, and their respective types
# If nothing scheduled for the day, stop executing
# Store in daily parquet files

# Questions:
# - How do we make sure the API is actually working on a given day -> retry, response code checks
# - How do you pass the response of the API call to our pipeline? -> XCom
# - Do we need to store anything locally? -> the parquet files
# - How do you tell API that you need data for only one date? -> filters

# Store it int secure and scalable cloud storage solution
# Integrate with BigQuery
# Optionally: Integrate in PostgreSQL

# https://lldev.thespacedevs.com/2.2.0/launch/?
# window_start__gte=2023-10-02T00%3A00%3A00Z
# window_end__lt=2023-10-03T00%3A00%3A00Z

#{
#    "count": 1,
#    "next": null,
#    "previous": null,
#    "results": []
#}

from airflow.models import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator, ShortCircuitOperator
from airflow.operators.http_operator import SimpleHttpOperator
from airflow.sensors.http_sensor import HttpSensor
from airflow.exceptions import AirflowSkipException
from airflow.providers.google.cloud.operators.bigquery import BigQueryCreateEmptyDatasetOperator, BigQueryCreateEmptyTableOperator
from airflow.providers.google.cloud.transfers.local_to_gcs import LocalFilesystemToGCSOperator
from airflow.providers.google.cloud.transfers.gcs_to_bigquery import GCSToBigQueryOperator
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.hooks.postgres_hook import PostgresHook
import pendulum
import json
import pandas as pd

CONN_GOOGLE = "google_cloud_conn"
BIGQUERY_PROJECT_ID = "aflow-training-rabo-2023-10-02"
BIGQUERY_DATASET_ID = "dimas_dataset"
BIGQUERY_TABLE_ID = "daily_rocket_launches"

class NoRocketLaunchTodayException(Exception):
    pass

with DAG(
    dag_id="the_big_launch_detector_dag",
    start_date=pendulum.today("UTC").add(days=-10)
) as dag:
    check_api_status = HttpSensor(
        task_id="check_api_status",
        http_conn_id="thespacedevs_dev",
        endpoint="",
        #request_params={
        #    "window_start__gte": "2023-10-02T00:00:00Z",
        #    "window_end__lt": "2023-10-03T00:00:00Z"
        #},
        method="GET",
        response_check=lambda response: response.status_code == 200,
        mode="poke",
        timeout=300,
        poke_interval=30,
        dag=dag
    )


    get_rocket_launches_today = SimpleHttpOperator(
        task_id="get_rocket_launches_today",
        http_conn_id="thespacedevs_dev",
        method="GET",
        endpoint="",
        data={
            "net__gte": "{{ ds }}T00:00:00Z",
            "net__lt": "{{ next_ds }}T00:00:00Z"
        },
        #headers={"Content-Type": "application/json"},
        #response_check=lambda response: response.json()["count"] > 0,
        dag=dag
    )


    def _check_launches_in_request(task_instance, **context):
        response = task_instance.xcom_pull(task_ids="get_rocket_launches_today", key="return_value")
        response_dict = json.loads(response)

        if response_dict["count"] < 1:
            raise AirflowSkipException("No launch is scheduled for today.")


    check_if_a_launch_is_scheduled_for_today = PythonOperator(
        task_id="check_if_a_launch_is_scheduled_for_today",
        python_callable=_check_launches_in_request
    )


    def _extract_relevant_data(x: dict):
        return {
            "id": x.get("id"),
            "name": x.get("name"),
            "status": x.get("status").get("abbrev"),
            "country_code": x.get("pad").get("country_code"),
            "service_provider_name": x.get("launch_service_provider").get("name"),
            "service_provider_type": x.get("launch_service_provider").get("type")
        }


    def _preprocess_data(task_instance, **context):
        response = task_instance.xcom_pull(task_ids="get_rocket_launches_today", key="return_value")
        response_dict = json.loads(response)["results"]
        df_results = pd.DataFrame([_extract_relevant_data(x) for x in response_dict])
        df_results.to_parquet(path=f"/tmp/{context['ds']}.parquet")


    preprocess = PythonOperator(
        task_id="preprocess_launch_data",
        python_callable=_preprocess_data
    )

    create_empty_dataset_bigquery = BigQueryCreateEmptyDatasetOperator(
        task_id="create_empty_dataset_bigquery",
        gcp_conn_id = CONN_GOOGLE,
        project_id=BIGQUERY_PROJECT_ID,
        dataset_id=BIGQUERY_DATASET_ID,
    )

    create_empty_table_bigquery = BigQueryCreateEmptyTableOperator(
        task_id="create_empty_table_bigquery",
        gcp_conn_id = CONN_GOOGLE,
        project_id=BIGQUERY_PROJECT_ID,
        dataset_id=BIGQUERY_DATASET_ID,
        table_id=BIGQUERY_TABLE_ID,
        schema_fields=[
            {"name": "id", "type": "STRING", "mode": "REQUIRED"},
            {"name": "name", "type": "STRING", "mode": "REQUIRED"},
            {"name": "status", "type": "STRING", "mode": "REQUIRED"},
            {"name": "country_code", "type": "STRING", "mode": "REQUIRED"},
            {"name": "service_provider_name", "type": "STRING", "mode": "REQUIRED"},
            {"name": "service_provider_type", "type": "STRING", "mode": "REQUIRED"},
        ],
    )

    upload_parquet_to_gcs = LocalFilesystemToGCSOperator(
        task_id="upload_parquet_to_gcs",
        gcp_conn_id = CONN_GOOGLE,
        src="/tmp/{{ ds }}.parquet",
        dst="dimas/{{ ds }}.parquet",
        bucket=BIGQUERY_PROJECT_ID,
    )

    write_parquet_to_bq = GCSToBigQueryOperator(
        task_id='write_parquet_to_bq',
        gcp_conn_id = CONN_GOOGLE,
        bucket=BIGQUERY_PROJECT_ID,
        source_objects=["dimas/{{ ds }}.parquet"],
        source_format="parquet",
        destination_project_dataset_table=f"{BIGQUERY_DATASET_ID}.{BIGQUERY_TABLE_ID}",
        write_disposition='WRITE_APPEND',
    )

    create_postgres_table = PostgresOperator(
        task_id="create_postgres_table",
        postgres_conn_id="postgres",
        sql="""
        CREATE TABLE IF NOT EXISTS rocket_launches (
        id VARCHAR,
        name VARCHAR,
        status VARCHAR,
        country_code VARCHAR,
        service_provider_name VARCHAR,
        service_provider_type VARCHAR
        );
        """
    )

    def _read_parquet_and_write_to_postgres(task_instance, **context):
        hook = PostgresHook(postgres_conn_id="postgres")

        # Read data from parquet
        df_launches = pd.read_parquet(f"/tmp/{context['ds']}.parquet")
        df_launches.to_csv(f"/tmp/{context['ds']}.csv", header=False, index=False)

        hook.copy_expert(f"COPY rocket_launches (id, name, status, country_code, service_provider_name, service_provider_type) FROM STDIN WITH CSV DELIMITER AS ','", f"/tmp/{context['ds']}.csv")

    write_parquet_to_postgres = PythonOperator(
        task_id="write_parquet_to_postgres",
        python_callable=_read_parquet_and_write_to_postgres
    )

(
    check_api_status >> 
    get_rocket_launches_today >> 
    check_if_a_launch_is_scheduled_for_today >> 
    preprocess >> 
    create_empty_dataset_bigquery >> 
    create_empty_table_bigquery >> 
    upload_parquet_to_gcs >> 
    write_parquet_to_bq >>
    create_postgres_table >>
    write_parquet_to_postgres
)