from airflow.sdk import dag, TaskGroup
from airflow.providers.google.cloud.operators.bigquery import BigQueryInsertJobOperator
from airflow.providers.google.cloud.transfers.bigquery_to_gcs import (
    BigQueryToGCSOperator,
)
from airflow.providers.google.cloud.transfers.gcs_to_bigquery import (
    GCSToBigQueryOperator,
)
from airflow.providers.standard.operators.bash import BashOperator
import pendulum
import os

GCP_CONN_ID = "google_cloud_default"
BQ_PROJECT = os.environ.get("GCP_PROJECT_ID")
GCP_BUCKET = os.environ.get("GCS_RAW_BUCKET")
DATASET_REGION = os.environ.get("GCP_REGION")
RAW_DATASET = os.environ.get("BQ_RAW_DATASET")
YEARS = list(range(2008, 2023))


@dag(
    dag_id="stackoverflow",
    schedule="@monthly",
    start_date=pendulum.datetime(2024, 1, 1, tz="UTC"),
    catchup=False,
    max_active_tasks=8,
    tags=["stackoverflow"],
)
def stackoverflow_pipeline():

    def build_export_config(table, year):
        return {
            "query": {
                "query": f"""
                    EXPORT DATA OPTIONS (
                        uri='gs://{GCP_BUCKET}/{table}/year={year}/*.parquet',
                        format='PARQUET',
                        overwrite=true
                    ) AS
                    SELECT
                        *
                    FROM `bigquery-public-data.stackoverflow.{table}`
                    WHERE EXTRACT(YEAR FROM creation_date) = {year}
            """,
                "useLegacySql": False,
            }
        }

    with TaskGroup("extract_to_gcs") as extract:
        BigQueryInsertJobOperator.partial(
            task_id="export_posts_questions",
            gcp_conn_id=GCP_CONN_ID,
            project_id=BQ_PROJECT,
            location="US",
        ).expand(
            configuration=[
                build_export_config("posts_questions", year) for year in YEARS
            ]
        )

        BigQueryInsertJobOperator.partial(
            task_id="export_posts_answers",
            gcp_conn_id=GCP_CONN_ID,
            project_id=BQ_PROJECT,
            location="US",
        ).expand(
            configuration=[build_export_config("posts_answers", year) for year in YEARS]
        )

        BigQueryToGCSOperator(
            task_id="export_users",
            source_project_dataset_table="bigquery-public-data.stackoverflow.users",
            destination_cloud_storage_uris=[
                f"gs://{GCP_BUCKET}/users/snapshot_date={{{{ ds }}}}/data_*.parquet"
            ],
            export_format="PARQUET",
            gcp_conn_id=GCP_CONN_ID,
        )

        BigQueryToGCSOperator(
            task_id="export_tags",
            source_project_dataset_table="bigquery-public-data.stackoverflow.tags",
            destination_cloud_storage_uris=[
                f"gs://{GCP_BUCKET}/tags/snapshot_date={{{{ ds }}}}/data_*.parquet"
            ],
            export_format="PARQUET",
            gcp_conn_id=GCP_CONN_ID,
        )

    with TaskGroup("load_to_bigquery") as load:
        BigQueryInsertJobOperator(
            task_id="load_posts_questions",
            configuration={
                "load": {
                    "sourceUris": [
                        f"gs://{GCP_BUCKET}/posts_questions/year={year}/*.parquet"
                        for year in YEARS
                    ],
                    "destinationTable": {
                        "projectId": BQ_PROJECT,
                        "datasetId": RAW_DATASET,
                        "tableId": "posts_questions",
                    },
                    "sourceFormat": "PARQUET",
                    "writeDisposition": "WRITE_TRUNCATE",
                    "createDisposition": "CREATE_IF_NEEDED",
                    "autodetect": True,
                }
            },
            gcp_conn_id=GCP_CONN_ID,
            project_id=BQ_PROJECT,
            location=DATASET_REGION,
        )

        BigQueryInsertJobOperator(
            task_id="load_posts_answers",
            configuration={
                "load": {
                    "sourceUris": [
                        f"gs://{GCP_BUCKET}/posts_answers/year={year}/*.parquet"
                        for year in YEARS
                    ],
                    "destinationTable": {
                        "projectId": BQ_PROJECT,
                        "datasetId": RAW_DATASET,
                        "tableId": "posts_answers",
                    },
                    "sourceFormat": "PARQUET",
                    "writeDisposition": "WRITE_TRUNCATE",
                    "createDisposition": "CREATE_IF_NEEDED",
                    "autodetect": True,
                }
            },
            gcp_conn_id=GCP_CONN_ID,
            project_id=BQ_PROJECT,
            location=DATASET_REGION,
        )

        GCSToBigQueryOperator(
            task_id="load_users",
            bucket=GCP_BUCKET,
            source_objects=["users/snapshot_date={{ ds }}/data_*.parquet"],
            destination_project_dataset_table=f"{BQ_PROJECT}.{RAW_DATASET}.users",
            source_format="PARQUET",
            write_disposition="WRITE_TRUNCATE",
            autodetect=True,
            location=DATASET_REGION,
            project_id=BQ_PROJECT,
            gcp_conn_id=GCP_CONN_ID,
        )

        GCSToBigQueryOperator(
            task_id="load_tags",
            bucket=GCP_BUCKET,
            source_objects=["tags/snapshot_date={{ ds }}/data_*.parquet"],
            destination_project_dataset_table=f"{BQ_PROJECT}.{RAW_DATASET}.tags",
            source_format="PARQUET",
            write_disposition="WRITE_TRUNCATE",
            autodetect=True,
            location=DATASET_REGION,
            project_id=BQ_PROJECT,
            gcp_conn_id=GCP_CONN_ID,
        )

    with TaskGroup("transform") as transform:
        dbt_deps = BashOperator(
            task_id="dbt_deps",
            bash_command="cd /opt/airflow/dbt/stackoverflow && dbt deps --profiles-dir /opt/airflow/dbt",
        )

        dbt_staging = BashOperator(
            task_id="dbt_run_staging",
            bash_command="cd /opt/airflow/dbt/stackoverflow && dbt run --select staging --profiles-dir /opt/airflow/dbt",
        )

        dbt_intermediate = BashOperator(
            task_id="dbt_run_intermediate",
            bash_command="cd /opt/airflow/dbt/stackoverflow && dbt run --select intermediate --profiles-dir /opt/airflow/dbt",
        )

        dbt_mart = BashOperator(
            task_id="dbt_run_mart",
            bash_command="cd /opt/airflow/dbt/stackoverflow && dbt run --select marts --profiles-dir /opt/airflow/dbt",
        )

        dbt_deps >> dbt_staging >> dbt_intermediate >> dbt_mart

    extract >> load >> transform


dag = stackoverflow_pipeline()
