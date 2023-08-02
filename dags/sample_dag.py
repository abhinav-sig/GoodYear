"""
### Run a dbt Core project as a task group with Cosmos

Simple DAG showing how to run a dbt project as a task group, using
an Airflow connection and injecting a variable into the dbt project.
"""

from airflow.decorators import dag
from cosmos.providers.dbt.task_group import DbtTaskGroup
from pendulum import datetime
from airflow.providers.snowflake.transfers.s3_to_snowflake import S3ToSnowflakeOperator
from airflow.utils.task_group import TaskGroup

CONNECTION_ID = "snowflake_stage_id"
DB_NAME = "GOOD_YEAR"
SCHEMA_NAME = "STAGE"
DBT_PROJECT_NAME = "dbt_project"
# the path where Cosmos will find the dbt executable
# in the virtual environment created in the Dockerfile
DBT_EXECUTABLE_PATH = "/usr/local/airflow/dbt_venv/bin/dbt"
# The path to your dbt root directory
DBT_ROOT_PATH = "/usr/local/airflow/dags/dbt"



@dag(
    start_date=datetime(2023, 6, 1),
    schedule=None,
    catchup=False,
)
def my_simple_dbt_dag():


    with TaskGroup(group_id='s3_to_snowflake') as tg_s3_to_snowflake:

        copy_into_crm1 = S3ToSnowflakeOperator(
            task_id="copy_into_crm1",
            snowflake_conn_id=CONNECTION_ID,
            # s3_keys=["crm.csv"],
            table="RAW_CRM",
            stage="CRM",
            file_format="(type = csv ,field_delimiter = ',',skip_header=1,null_if = ('NULL', 'null'), empty_field_as_null = true,FIELD_OPTIONALLY_ENCLOSED_BY='\"')"
        )
        copy_into_device1 = S3ToSnowflakeOperator(
            task_id="copy_into_device1",
            snowflake_conn_id=CONNECTION_ID,
            # s3_keys=["device1.csv"],
            table="RAW_DEVICE",
            stage="DEVICE",
            file_format="(type = csv ,field_delimiter = ',',skip_header=1,null_if = ('NULL', 'null'), empty_field_as_null = true,FIELD_OPTIONALLY_ENCLOSED_BY='\"')"
        )
        copy_into_rev1 = S3ToSnowflakeOperator(
            task_id="copy_into_rev1",
            snowflake_conn_id=CONNECTION_ID,
            # s3_keys=["rev1.csv"],
            table="RAW_REV",
            stage="REV",
            file_format="(type = csv ,field_delimiter = ',',skip_header=1,null_if = ('NULL', 'null'), empty_field_as_null = true,FIELD_OPTIONALLY_ENCLOSED_BY='\"')"
        )
        [copy_into_crm1,copy_into_device1,copy_into_rev1]

    curated_layer=DbtTaskGroup(
        group_id="transform_data",
        dbt_project_name=DBT_PROJECT_NAME,
        conn_id=CONNECTION_ID,
        dbt_root_path=DBT_ROOT_PATH,
        dbt_args={
            "dbt_executable_path": DBT_EXECUTABLE_PATH,
            "schema": SCHEMA_NAME,
        },
    )

    tg_s3_to_snowflake>>curated_layer


my_simple_dbt_dag()