import json
from datetime import datetime, timedelta

from airflow import DAG
from airflow.models import Variable
from airflow.operators.dummy_operator import DummyOperator
#from airflow.operators.python_operator import PythonOperator
from airflow.operators import (    
    StageToS3Operator, 
    GlueCrawlerOperator
)
from airflow.contrib.operators.emr_create_job_flow_operator import EmrCreateJobFlowOperator
from airflow.contrib.operators.emr_add_steps_operator import EmrAddStepsOperator
from airflow.contrib.sensors.emr_step_sensor import EmrStepSensor
from airflow.contrib.operators.emr_terminate_job_flow_operator import EmrTerminateJobFlowOperator



## Airflow Config Variables
# EMR cluster creation
job_flow_overrides = Variable.get("job_flow_overrides")
cluster_name = Variable.get("cluster_name")
log_uri = Variable.get("log_uri")
ec2_key_name = Variable.get("ec2_key_name")
ec2_subnet_id = Variable.get("ec2_subnet_id")
job_flow_role = Variable.get("job_flow_role")
service_role = Variable.get("service_role")
# AWS Variables
bucket_name = Variable.get("bucket_name")
region_name = Variable.get("region_name")
glue_endpoint_url = Variable.get("glue_endpoint_url")


# data / scripts to be copied to S3
local_data = ["./staging/"]
local_scripts = ["./dags/etl-prod.py", 
                 "./dags/dq-prod.py"]

# s3 script location
s3_etl_script = "scripts/etl-prod.py"
s3_dq_script = "scripts/dq-prod.py"


# SPARK STEPS
SPARK_STEPS = [
    {
        "Name": "Staging to Model",
        "ActionOnFailure": "CANCEL_AND_WAIT",
        "HadoopJarStep": {
            "Jar": "command-runner.jar",
            "Args": [
                "spark-submit",
                "--deploy-mode",
                "client",
                "--packages",
                "saurfang:spark-sas7bdat:2.1.0-s_2.11",
                "s3://{{ params.bucket_name }}/{{ params.s3_etl_script }}",
                "{{ params.bucket_name }}",
                "i94_{{ prev_execution_date.year }}-{{ '{:02}'.format(prev_execution_date.month) }}_sub.sas7bdat"
            ],
        },
    },
    {
        "Name": "Data Quality",
        "ActionOnFailure": "CANCEL_AND_WAIT",
        "HadoopJarStep": {
            "Jar": "command-runner.jar",
            "Args": [
                "spark-submit",
                "--deploy-mode",
                "client",
                "s3://{{ params.bucket_name }}/{{ params.s3_dq_script }}",
                "{{ params.bucket_name }}",
                "{{ prev_execution_date.year }}",
                "{{ prev_execution_date.month }}"
            ],
        },
    }
]


# read cluster definition
def get_job_flow_overrides(filename):
    with open(filename, "r") as f:
        data = f.read()
    
    jobflow = json.loads(data)
    return jobflow


default_args = {
    "owner": "marcus",
    "depends_on_past": False,
    "wait_for_downstream": True,
    "email_on_retry": False,
    "retries": 0,
    "retry_delay": timedelta(minutes=5),
    "start_date": datetime(2016, 7, 1),
    "end_date": datetime(2016, 7, 15)
}


with DAG(
    "immigration_model_data_load",
    default_args=default_args,
    schedule_interval="@monthly",
    catchup=True,
    max_active_runs=1,
) as dag:
    
    # dummy start operator
    start_data_pipeline = DummyOperator(task_id="start_data_pipeline")
    
    # read local data and transfer to S3
    load_data = StageToS3Operator(
        task_id="load_data_to_S3",
        mode="staging",
        filename=local_data,
        bucket_name=bucket_name,
        prefix="staging",
        key="",
        load_sas=False,
        provide_context=True
        )

    # read local script files and transfer to S3
    load_script = StageToS3Operator(
        task_id="load_script_to_S3",
        mode="scripts",
        filename=local_scripts,
        bucket_name=bucket_name,
        prefix="scripts",
        key=""
        )

    # Create an EMR cluster
    create_emr_cluster = EmrCreateJobFlowOperator(
        task_id="create_emr_cluster",
        job_flow_overrides=get_job_flow_overrides(job_flow_overrides),
        aws_conn_id="aws_default",
        emr_conn_id="emr_default"
    )

    # Add steps to the EMR cluster
    # Step 1 = ETL Pipeline
    # Step 2 = Data Quality Test
    step_adder = EmrAddStepsOperator(
        task_id="add_steps",
        job_flow_id="{{ task_instance.xcom_pull(task_ids='create_emr_cluster', key='return_value') }}",
        aws_conn_id="aws_default",
        steps=SPARK_STEPS,
        params={
            "bucket_name": bucket_name,
            "s3_etl_script": s3_etl_script,
            "s3_dq_script": s3_dq_script
        }
    )


    # wait for the steps to complete
    last_step = len(SPARK_STEPS) - 1
    step_checker = EmrStepSensor(
        task_id="watch_step",
        job_flow_id="{{ task_instance.xcom_pull('create_emr_cluster', key='return_value') }}",
        step_id="{{ task_instance.xcom_pull(task_ids='add_steps', key='return_value')["
        + str(last_step)
        + "] }}",
        aws_conn_id="aws_default",
    )

    # Terminate the EMR cluster
    terminate_emr_cluster = EmrTerminateJobFlowOperator(
        task_id="terminate_emr_cluster",
        job_flow_id="{{ task_instance.xcom_pull(task_ids='create_emr_cluster', key='return_value') }}",
        aws_conn_id="aws_default"
    )
    
    # Run Glue Crawler to update Athena
    run_glue_crawler = GlueCrawlerOperator(
        task_id = "S3_to_athena_crawl",
        crawler_name = "model_crawler",
        region_name = region_name,
        endpoint_url = glue_endpoint_url
    )
    
    # dummy end operator
    end_data_pipeline = DummyOperator(task_id="end_data_pipeline")


    start_data_pipeline >> [load_data, load_script] >> create_emr_cluster
    create_emr_cluster >> step_adder >> step_checker >> terminate_emr_cluster
    terminate_emr_cluster >> run_glue_crawler >> end_data_pipeline
