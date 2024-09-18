from airflow import DAG
from airflow.providers.amazon.aws.operators.glue import GlueJobOperator
from airflow.utils.dates import days_ago
from datetime import timedelta

# Default arguments
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': days_ago(1),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5)
}

# Define the DAG
dag = DAG(
    'glue_job_integration',
    default_args=default_args,
    description='Trigger AWS Glue job from Airflow',
    schedule_interval=timedelta(hours=1), 
    catchup=False
)

# Define the Glue Job Operator
glue_job = GlueJobOperator(
    task_id='run_glue_job',
    job_name='api data transfromation', 
    aws_conn_id='aws_default',     
    dag=dag
)

# Define task dependencies
glue_job
