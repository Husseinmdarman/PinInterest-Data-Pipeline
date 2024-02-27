# Databricks notebook source
from airflow import DAG
from airflow.providers.databricks.operators.databricks import DatabricksSubmitRunOperator, DatabricksRunNowOperator
from datetime import datetime, timedelta 
from airflow.utils.trigger_rule import TriggerRule
from airflow.utils.task_group import TaskGroup

# Define the notebook paths
mounting_s3_bucket = "/Users/husseinmdarman@gmail.com/MountingS3Bucket"
loading_uncleaned_tables = "/Users/husseinmdarman@gmail.com/LoadingUncleanedTables"
cleaning_df_user = "/Users/husseinmdarman@gmail.com/CleaningUserDF"
cleaning_df_pin = "/Users/husseinmdarman@gmail.com/CleaningPinDF"
cleaning_df_geo = "/Users/husseinmdarman@gmail.com/CleaningGeoDF"
business_questions = "/Users/husseinmdarman@gmail.com/Questions Answered About Pinterest Data"

# Define the default arguments for the DAG
default_args = {
    "owner": "user",
    "depends_on_past": True,
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 3,
    "retry_delay": timedelta(minutes=2),
}

# Define the DAG
with DAG(
    "12e255fc4fcd_dag",
    start_date=datetime(2024, 2, 24),
    schedule_interval="@daily",
    default_args=default_args,
) as dag:

    # Define the task to run the mounting of the s3 buckets
    run_mount_bucket = DatabricksSubmitRunOperator(
        task_id="mounting_s3_bucket",
        databricks_conn_id="databricks_default",
        existing_cluster_id="1108-162752-8okw8dgg",
        notebook_task={"notebook_path": mounting_s3_bucket},
    )
    
    # Define the tasks to run the loading of the uncleaned tables
    run_loading_uncleaned_tables = DatabricksSubmitRunOperator(
        task_id="run_loading_uncleaned_tables",
        databricks_conn_id="databricks_default",
        existing_cluster_id="1108-162752-8okw8dgg",
        notebook_task={"notebook_path": loading_uncleaned_tables},
    )

    # Define the task to run the answering of the business questions
    run_business_questions = DatabricksSubmitRunOperator(
        task_id="run_business_questions",
        databricks_conn_id="databricks_default",
        existing_cluster_id="1108-162752-8okw8dgg",
        notebook_task={"notebook_path": business_questions},
        trigger_rule= TriggerRule.NONE_FAILED
    
    )

    #cleaning task group, that handles all dataframe cleaning
    with TaskGroup("cleaning_tasks") as cleaning_tasks:

        run_cleaning_df_user = DatabricksSubmitRunOperator(
        task_id="run_cleaning_df_user",
        databricks_conn_id="databricks_default",
        existing_cluster_id="1108-162752-8okw8dgg",
        notebook_task={"notebook_path": cleaning_df_user},
        )
        run_cleaning_df_pin = DatabricksSubmitRunOperator(
        task_id="run_cleaning_df_pin",
        databricks_conn_id="databricks_default",
        existing_cluster_id="1108-162752-8okw8dgg",
        notebook_task={"notebook_path": cleaning_df_pin},
        )
        run_cleaning_df_geo = DatabricksSubmitRunOperator(
        task_id="run_cleaning_df_geo",
        databricks_conn_id="databricks_default",
        existing_cluster_id="1108-162752-8okw8dgg",
        notebook_task={"notebook_path": cleaning_df_geo},
        )
        [run_cleaning_df_geo >> run_cleaning_df_pin >> run_cleaning_df_user]
    
    #task dependencies
    run_mount_bucket >> run_loading_uncleaned_tables >> cleaning_tasks >> run_business_questions
