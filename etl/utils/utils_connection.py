import os
from dotenv import load_dotenv
from sqlalchemy import create_engine
from sqlalchemy.exc import SQLAlchemyError
import boto3
import logging
from botocore.exceptions import NoCredentialsError, PartialCredentialsError

# Load environment variables from .env file
load_dotenv()

# Retrieve individual components from environment variables for Postgres
user = os.getenv('POSTGRES_USER')
password = os.getenv('POSTGRES_PASSWORD')
host = os.getenv('POSTGRES_HOST')
port = os.getenv('POSTGRES_PORT', '5432')  # Default Postgres port
db_name = os.getenv('POSTGRES_DB')

# Ensure the connection URI is retrieved successfully
if not all([user, password, host, db_name]):
    raise ValueError("One or more environment variables for the database connection are not set")

# Construct the connection URI
connection_uri = f"postgresql://{user}:{password}@{host}:{port}/{db_name}"

def get_connection_uri() -> str:
    """
    Get the database connection URI.

    Returns:
        str: The connection URI for the database.
    """
    return connection_uri

def create_db_engine(connection_uri: str) -> any:
    """
    Create and return a SQLAlchemy engine based on the provided connection URI.

    Args:
        connection_uri (str): The connection URI for the database.

    Returns:
        Engine: A SQLAlchemy engine connected to the specified database.
    """
    try:
        db_engine = create_engine(connection_uri)
        logging.info("Database engine created successfully.")
        return db_engine
    except SQLAlchemyError as e:
        logging.error(f"Error occurred while creating the database engine: {str(e)}")
        return None
    except Exception as e:
        logging.error(f"An unexpected error occurred: {str(e)}")
        return None

def create_s3_client() -> boto3.client:
    """
    Create and return a boto3 S3 client.

    Returns:
        boto3.client: A boto3 S3 client object.
    """
    # Fetch AWS credentials from environment variables
    s3_access_key_id = os.getenv('S3_ACCESS_KEY_ID')
    s3_secret_access_key = os.getenv('S3_SECRET_ACCESS_KEY')
    s3_region = os.getenv('S3_REGION')

    try:
        session = boto3.Session(
            aws_access_key_id=s3_access_key_id,
            aws_secret_access_key=s3_secret_access_key,
            region_name=s3_region
        )
        s3_client = session.client('s3')
        print("S3 client created successfully.")
        return s3_client
    except NoCredentialsError:
        raise ValueError("AWS credentials not found.")
    except PartialCredentialsError:
        raise ValueError("Incomplete AWS credentials.")
    except Exception as e:
        raise ValueError(f"Error initializing S3 client: {e}")

def get_s3_bucket_name() -> str:
    """
    Retrieve the S3 bucket name from the environment variable.

    Returns:
        str: The name of the S3 bucket.
    """
    bucket_name = os.getenv('S3_BUCKET_NAME')
    if not bucket_name:
        raise ValueError("S3_BUCKET_NAME environment variable is not set.")
    return bucket_name

def get_s3_parquet_file_key() -> str:
    """
    Retrieve the Parquet file key from the environment variable.

    Returns:
        str: The S3 key for the Parquet file.
    """
    parquet_file_key = os.getenv('S3_PARQUET_FILE')
    if not parquet_file_key:
        raise ValueError("S3_PARQUET_FILE environment variable is not set.")
    return parquet_file_key

def list_s3_objects(s3_client: boto3.client) -> list:
    """
    List objects in an S3 bucket.

    Args:
        s3_client (boto3.client): The S3 client.

    Returns:
        list: A list of object keys in the specified S3 bucket.
    """
    # Use the bucket name from the environment variable
    bucket_name = get_s3_bucket_name()

    objects = []
    try:
        response = s3_client.list_objects_v2(Bucket=bucket_name)
        if 'Contents' in response:
            objects = [obj['Key'] for obj in response['Contents']]
    except Exception as e:
        logging.error(f"Error listing S3 objects: {e}")
        raise
    return objects

def get_sftp_files_prefix() -> str:
    """
    Retrieve the S3 SFTP files prefix from the environment variable.

    Returns:
        str: The S3 SFTP files prefix.
    """
    sftp_prefix = os.getenv('S3_SFTP_FILES_PREFIX')
    if not sftp_prefix:
        raise ValueError("S3_SFTP_FILES_PREFIX environment variable is not set.")
    return sftp_prefix