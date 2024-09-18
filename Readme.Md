# Reddit Data Pipeline

## Overview

This project sets up a real-time data pipeline to fetch data from Reddit, transform it using AWS Glue, and store it in Amazon S3. This involves data streaming, cloud storage, ETL (Extract, Transform, Load) processes, and orchestration using Apache Airflow.


## Prerequisites

- **Python**: Version 3.x
- **Docker**: For local development and orchestration
- **AWS Account**: For S3 and AWS Glue
- **Reddit API Credentials**: For accessing Reddit data
- **Aws Iam policy user key**: For writing and reading data from data source

## Setup

### 1. Clone the Repository

```bash
git clone https://github.com/olusimeon/reddit-data-pipeline.git
cd reddit-data-pipeline
```


### 2. Configure Environment Variables 

```makefile
REDDIT_CLIENT_ID=your_reddit_client_id
REDDIT_CLIENT_SECRET=your_reddit_client_secret
REDDIT_USER_AGENT=your_reddit_user_agent
AWS_ACCESS_KEY_ID=your_aws_access_key_id
AWS_SECRET_ACCESS_KEY=your_aws_secret_access_key
```

### 3 Install Dependencies
```bash
pip install -r requiremnts.txt
```

### 4 Set up Docker 
You can download docker.compose.yaml from the offical apache airflow website [apache airflow](https://airflow.apache.org/docs/apache-airflow/2.10.1/docker-compose.yaml). or download the docker-compose.yaml provided in this folder


### 5. Deploy and Run the Pipeline
Airflow: Access the Airflow web interface at http://localhost:8080 and trigger the DAG named pull_comments_and_load.py.
AWS Glue: Ensure the Glue job script (glue/api transform.py) is configured and deployed to your AWS Glue environment using the script and python shell environment.

### 6. Usage

ETL Process: The Airflow DAG (airflow/dags/pullcomments.py) schedules the ETL pipeline, fetching data from Reddit, triggering AWS Glue for transformation, and storing results in S3.
Transformation: The AWS Glue job script (glue/api transform.py) processes and transforms the raw data from S3.
S3 Bucket Structure
Refer to s3/bucket_structure.txt for details on the organization of S3 buckets and directories used in this project.

### Additional Notes
Ensure your AWS IAM roles and permissions are correctly configured for accessing S3 and running Glue jobs.
Regularly monitor Airflow logs and AWS Glue job logs for any issues or errors in the pipeline execution.

### Contributing
Feel free to submit issues or pull requests. Contributions are welcome!

### Contact
For any questions or comments, please reach out to olusimeon4@gmail.com.

