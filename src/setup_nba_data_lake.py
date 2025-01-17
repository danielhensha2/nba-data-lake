import boto3
import json
import time
import requests
from dotenv import load_dotenv
import os
import uuid
import logging

# Configure logging
logging.basicConfig(level=logging.INFO)

# Load environment variables from .env file
load_dotenv()

# AWS configurations
region = "us-east-1"  # Replace with your preferred AWS region
bucket_name = f"sports-analytics-data-lake-{uuid.uuid4().hex[:8]}"  # Unique S3 bucket name
glue_database_name = "glue_nba_data_lake"
athena_output_location = f"s3://{bucket_name}/athena-results/"

# Sportsdata.io configurations
api_key = os.getenv("SPORTS_DATA_API_KEY")
nba_endpoint = os.getenv("NBA_ENDPOINT")

# Validate environment variables
if not api_key or not nba_endpoint:
    raise ValueError("API key or NBA endpoint missing. Check your .env file.")

# Create AWS clients
s3_client = boto3.client("s3", region_name=region)
glue_client = boto3.client("glue", region_name=region)
athena_client = boto3.client("athena", region_name=region)

def create_s3_bucket():
    try:
        if region == "us-east-1":
            s3_client.create_bucket(Bucket=bucket_name)
        else:
            s3_client.create_bucket(
                Bucket=bucket_name,
                CreateBucketConfiguration={"LocationConstraint": region},
            )
        logging.info(f"S3 bucket '{bucket_name}' created successfully.")
    except Exception as e:
        logging.error(f"Error creating S3 bucket: {e}")
        raise

def create_glue_database():
    """Create a Glue database for the data lake."""
    try:
        existing_databases = glue_client.get_databases()["DatabaseList"]
        if any(db["Name"] == glue_database_name for db in existing_databases):
            print(f"Glue database '{glue_database_name}' already exists. Skipping creation.")
            return
        glue_client.create_database(
            DatabaseInput={
                "Name": glue_database_name,
                "Description": "Glue database for NBA sports analytics.",
            }
        )
        print(f"Glue database '{glue_database_name}' created successfully.")
    except Exception as e:
        print(f"Error creating Glue database: {e}")


def fetch_nba_data():
    try:
        headers = {"Ocp-Apim-Subscription-Key": api_key}
        response = requests.get(nba_endpoint, headers=headers)
        response.raise_for_status()
        logging.info("Fetched NBA data successfully.")
        return response.json()
    except Exception as e:
        logging.error(f"Error fetching NBA data: {e}")
        return []

def convert_to_line_delimited_json(data):
    logging.info("Converting data to line-delimited JSON format...")
    return "\n".join([json.dumps(record) for record in data])

def upload_data_to_s3(data):
    try:
        file_key = "raw-data/nba_player_data.jsonl"
        s3_client.put_object(
            Bucket=bucket_name,
            Key=file_key,
            Body=convert_to_line_delimited_json(data)
        )
        logging.info(f"Uploaded data to S3: {file_key}")
    except Exception as e:
        logging.error(f"Error uploading data to S3: {e}")
        raise

def create_glue_table():
    """Create a Glue table for the data."""
    try:
        # Check if the table already exists
        existing_tables = glue_client.get_tables(DatabaseName=glue_database_name)["TableList"]
        if any(table["Name"] == "nba_players" for table in existing_tables):
            print("Glue table 'nba_players' already exists. Skipping creation.")
            return

        glue_client.create_table(
            DatabaseName=glue_database_name,
            TableInput={
                "Name": "nba_players",
                "StorageDescriptor": {
                    "Columns": [
                        {"Name": "PlayerID", "Type": "int"},
                        {"Name": "FirstName", "Type": "string"},
                        {"Name": "LastName", "Type": "string"},
                        {"Name": "Team", "Type": "string"},
                        {"Name": "Position", "Type": "string"},
                        {"Name": "Points", "Type": "int"}
                    ],
                    "Location": f"s3://{bucket_name}/raw-data/",
                    "InputFormat": "org.apache.hadoop.mapred.TextInputFormat",
                    "OutputFormat": "org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat",
                    "SerdeInfo": {
                        "SerializationLibrary": "org.openx.data.jsonserde.JsonSerDe"
                    },
                },
                "TableType": "EXTERNAL_TABLE",
            },
        )
        print("Glue table 'nba_players' created successfully.")
    except Exception as e:
        print(f"Error creating Glue table: {e}")

def configure_athena():
    try:
        athena_client.start_query_execution(
            QueryString="CREATE DATABASE IF NOT EXISTS nba_analytics",
            QueryExecutionContext={"Database": glue_database_name},
            ResultConfiguration={"OutputLocation": athena_output_location},
        )
        logging.info("Athena output location configured successfully.")
    except Exception as e:
        logging.error(f"Error configuring Athena: {e}")
        raise

def main():
    logging.info("Setting up data lake for NBA sports analytics...")
    try:
        create_s3_bucket()
        time.sleep(5)
        create_glue_database()
        nba_data = fetch_nba_data()
        if nba_data:
            upload_data_to_s3(nba_data)
        create_glue_table()
        configure_athena()
        logging.info("Data lake setup complete.")
    except Exception as e:
        logging.error(f"Setup failed: {e}")

if __name__ == "__main__":
    main()

