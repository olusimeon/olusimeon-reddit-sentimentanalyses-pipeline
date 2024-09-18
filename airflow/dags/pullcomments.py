from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.operators.dagrun_operator import TriggerDagRunOperator
from datetime import datetime, timedelta
import pandas as pd
import praw
import boto3
from io import BytesIO
from airflow.providers.amazon.aws.hooks.base_aws import AwsBaseHook

# Default DAG arguments
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2024, 9, 10),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5)
}

# Define DAG
dag = DAG(
    'pull_comments_and_load',
    default_args=default_args,
    description='Pull new comments and load data',
    schedule_interval=timedelta(hours=1),  # Run every hour
    catchup=False
)


# Task 1: Pull new comments from web
def pull_comments(**kwargs):
    try:
        # Initialize Reddit instance
        reddit = praw.Reddit(
            client_id='5T****VcQw', # reddit client if
            client_secret='518v*******5jw', # reddit client_secret
            user_agent='my_reddit_54' # reddit app name
        )

        # Fetch the post
        submission = reddit.submission(id='1fcwuig')
        submission.comments.replace_more()
        comments = submission.comments.list()

        comment_data_list = []
        for comment in comments:
            comment_data = {
                'Comment ID': comment.id,
                'Author': comment.author.name if comment.author else 'N/A',
                'Comment Body': comment.body,
                'Upvotes': comment.ups,
                'downvotes': comment.downs
            }
    
            comment_data_list.append(comment_data)
        
        #Push the comments data to XCom for the next task
        kwargs['ti'].xcom_push(key='comments', value=comment_data_list)

    except Exception as e:
        print(f"An error occurred: {e}")    
    
def load_data(**kwargs):
    try:
        #Retrieve the comment data from XCom
        comment_data_list = kwargs['ti'].xcom_pull(task_ids='pull_comments', key='comments')

    # Save DataFrame to a BytesIO object (in-memory stream)
        df = pd.DataFrame(comment_data_list)
        output = BytesIO()
        df.to_csv(output, index=False)
        output.seek(0)

        # Initialize S3 client using Airflow's AWS connection
        aws_hook = AwsBaseHook(aws_conn_id='aws_default', client_type='s3')
        s3 = aws_hook.get_client_type('s3')

        # Initialize S3 client
        s3 = boto3.client ('s3',
            aws_access_key_id="AKIAQ*****3J", ## aws acces key preferable to run this as an environment vairiable on docker
            aws_secret_access_key="oc0X+************Mm",
            region_name='us-east-1'  
        )

        s3.put_object(
            Bucket='sentimentanalysespipeline',
            Key='raw/commen.csv',
            Body=output,
            ContentType='application/vnd.openxmlformats-officedocument.spreadsheetml.sheet'
            )
        print("File uploaded successfully to S3.")

    except Exception as e:
        print(f"An error occured:{e}")
        

# Define the tasks
task_pull_comments = PythonOperator(
    task_id='pull_comments',
    python_callable=pull_comments,
    dag=dag
)
task_load_data = PythonOperator(
    task_id='load_data',
    python_callable=load_data,
    dag=dag
)

# Task to trigger the second DAG (for transforming the data)
trigger_glue_job = TriggerDagRunOperator(
    task_id='trigger_glue_job_integration',
    trigger_dag_id='glue_job_integration',  ## transpormation.py dag name
    wait_for_completion=True,  
    reset_dag_run=True  
    )

# Set task dependencies
task_pull_comments >> task_load_data >> trigger_glue_job
