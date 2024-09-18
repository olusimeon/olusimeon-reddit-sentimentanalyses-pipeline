import boto3
import pandas as pd
import os
from io import StringIO
import re

# Initialize S3 client
s3 = boto3.client('s3')

# Parameters (these can be passed dynamically)
SOURCE_BUCKET = os.environ.get('SOURCE_BUCKET', 'sentimentanalysespipeline')  
TARGET_BUCKET = os.environ.get('TARGET_BUCKET', 'sentimentanalysespipeline')  
RAW_DATA_KEY = 'raw/commen.csv' 
TRANSFORMED_DATA_KEY = 'processed/transform_data.csv'  

# Step 1: Read the raw data from S3 into a Pandas DataFrame
raw_data_object = s3.get_object(Bucket=SOURCE_BUCKET, Key=RAW_DATA_KEY)
raw_data = raw_data_object['Body'].read().decode('utf8',errors='replace')
process_data = pd.read_csv(StringIO(raw_data))

# Function to clean text
def clean_text(text):
    # Remove special characters, URLs, and extra whitespace
    text = re.sub(r"http\S+|www\S+|https\S+", '', text, flags=re.MULTILINE)  # Remove URLs
    text = re.sub(r'\@\w+|\#', '', text)  # Remove mentions (@) and hashtags (#)
    text = re.sub(r'[^A-Za-z0-9\s]+', '', text)  # Remove non-alphanumeric characters
    text = re.sub(r'\s+', ' ', text).strip()  # Remove extra spaces and leading/trailing spaces
    return text

# Clean the 'Comment Body' column
process_data['cleaned_comment_body'] = process_data['Comment Body'].apply(clean_text)

# Rename columns
process_data = process_data.rename(columns={"Comment ID": "comment_id", "User ID": "user_id", "Comment Body": "comment_body"})

# Drop the original 'Comment Body' column
process_data = process_data.drop(columns=["comment_body"])

#Write the transformed data back to S3
csv_buffer = StringIO()
process_data.to_csv(csv_buffer, index=False)

s3.put_object(Bucket=TARGET_BUCKET, Key=TRANSFORMED_DATA_KEY, Body=csv_buffer.getvalue())

print(f"Data transformation complete. Transformed data saved to s3://{TARGET_BUCKET}/{TRANSFORMED_DATA_KEY}")
