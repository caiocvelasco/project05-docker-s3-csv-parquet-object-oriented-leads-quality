# utils_checks_db.py

import pandas as pd  # Data Transformation
from sqlalchemy.exc import SQLAlchemyError
from sqlalchemy import text
from dotenv import load_dotenv
from utils_connection import create_db_engine, get_connection_uri  # Import the new function

# Load environment variables from .env file
load_dotenv()

# Function to check schema existence
def check_schema_existence(schema_names):
    try:
        connection_uri = get_connection_uri()  # Use the new function to get the connection URI
        db_engine = create_db_engine(connection_uri)  # Use the connection URI to create the engine
        if db_engine is None:
            print("Failed to create the database engine.")
            return
        
        with db_engine.connect() as connection:
            print("--- Checking if Schemas exist in the database ---")
            for schema_name in schema_names:
                result = connection.execute(
                    text("SELECT schema_name FROM information_schema.schemata WHERE schema_name = :schema"),
                    {"schema": schema_name}
                )
                schema_exists = result.fetchone() is not None
                if schema_exists:
                    print(f"Schema '{schema_name}' exists in the database.")
                else:
                    print(f"Schema '{schema_name}' does not exist in the database.")
            print("----- End of Schema Checking -----")
    
    except SQLAlchemyError as e:
        print(f"Error occurred while connecting to the database or executing query: {str(e)}")
    except Exception as e:
        print(f"An unexpected error occurred: {str(e)}")

# Function to check table existence
def check_table_existence(schema_name, table_names):
    try:
        connection_uri = get_connection_uri()  # Use the new function to get the connection URI
        db_engine = create_db_engine(connection_uri)  # Use the connection URI to create the engine
        if db_engine is None:
            print("Failed to create the database engine.")
            return
        
        with db_engine.connect() as connection:
            print("--- Checking if Tables exist ---")
            for table_name in table_names:
                result = connection.execute(
                    text("SELECT table_name FROM information_schema.tables WHERE table_schema = :schema AND table_name = :table"),
                    {"schema": schema_name, "table": table_name}
                )
                table_exists = result.fetchone() is not None
                if table_exists:
                    print(f"Table '{table_name}' exists in schema '{schema_name}'.")
                else:
                    print(f"Table '{table_name}' does not exist in schema '{schema_name}'.")
            print("----- End of Checking Tables -----")
    
    except SQLAlchemyError as e:
        print(f"Error occurred while connecting to the database or executing query: {str(e)}")
    except Exception as e:
        print(f"An unexpected error occurred: {str(e)}")

def map_bronze_columns(table_name):
    """
    Maps column names from CSV dataframes to corresponding column names in bronze tables.

    Args:
        table_name (str): Name of the table for which column mapping is required.

    Returns:
        dict: A dictionary mapping column names in CSV dataframes to their corresponding column names in bronze tables.
    """
    if table_name == 'leads_parquet':
        return {
            'lead_UUID': 'lead_uuid',
            'phone_hash': 'phone_hash',
            'email_hash': 'email_hash',
            'inserted_at': 'inserted_at'
        }

    elif table_name == 'csv_snapshots':
        return {
            'ENTRYDATE': 'entry_date',
            'LEADNUMBER': 'lead_number',
            'email_hash': 'email_hash',
            'phone_hash': 'phone_hash',
            'CITY': 'city',
            'STATE': 'state',
            'ZIP': 'zip',
            'APPT_DATE': 'appt_date',
            'Set': 'set_count',
            'Demo': 'demo',
            'Dispo': 'dispo',
            'JOB_STATUS': 'job_status',
            'inserted_at': 'inserted_at'
        }
    else:
        raise ValueError(f"Table '{table_name}' not found in the bronze layer.")

def get_schema_table_columns(connection_uri, schema_name, tables_in_schema):
    """
    Fetches column names for a set of tables in a specified schema from a database.

    Args:
        connection_uri (str): The database connection URI.
        schema_name (str): The schema name where the tables are located.
        tables_in_schema (list of str): A list of table names for which the column names are to be fetched.

    Returns:
        dict: A dictionary where the keys are table names and the values are lists of column names for each table.
    """
    columns_dict = {}
    try:
        engine = create_db_engine(connection_uri)
        if engine is None:
            print("Failed to create the database engine.")
        
        with engine.connect() as connection:
            for table_name in tables_in_schema:
                query = text(f"""
                    SELECT column_name 
                    FROM information_schema.columns 
                    WHERE table_schema = '{schema_name}' 
                    AND table_name = '{table_name}';
                """)
                result = connection.execute(query)
                columns = [row[0] for row in result]  # Extract the first element (column_name) and create a list of columns
                columns_dict[table_name] = columns  # Fill the columns_dict with keys (table_name) and values (list of column names) 

    except Exception as e:
        print(f"Error occurred while fetching view columns: {str(e)}")

    return columns_dict

def get_bronze_table_data_types():
    """
    Returns a dictionary with data types for columns in bronze tables.
    """
    bronze_data_types = {
        'leads_parquet': {
            'lead_UUID': 'UUID',
            'phone_hash': 'VARCHAR(255)',
            'email_hash': 'VARCHAR(255)',
            'inserted_at': 'TIMESTAMP'
        },
        'csv_snapshots': {
            'ENTRYDATE': 'DATE',
            'LEADNUMBER': 'INT',
            'email_hash': 'VARCHAR(255)',
            'phone_hash': 'VARCHAR(255)',
            'CITY': 'VARCHAR(100)',
            'STATE': 'CHAR(2)',
            'ZIP': 'VARCHAR(10)',
            'APPT_DATE': 'TIMESTAMP',
            'Set': 'INT',
            'Demo': 'INT',
            'Dispo': 'VARCHAR(50)',
            'JOB_STATUS': 'VARCHAR(100)',
            'inserted_at': 'TIMESTAMP'
        }
    }
    return bronze_data_types

def get_silver_table_data_types():
    """
    Returns a dictionary with data types for columns in silver tables.
    """
    silver_data_types = {
        'stg_leads_parquet': {
            'lead_uuid': 'UUID',
            'phone_hash': 'VARCHAR(255)',
            'email_hash': 'VARCHAR(255)',
            'inserted_at': 'TIMESTAMP'
        },
        'stg_csv_snapshots': {
            'entry_date': 'DATE',
            'lead_number': 'INT',
            'email_hash': 'VARCHAR(255)',
            'phone_hash': 'VARCHAR(255)',
            'city': 'VARCHAR(100)',
            'state': 'CHAR(2)',
            'zip': 'VARCHAR(10)',
            'appt_date': 'TIMESTAMP',
            'set_count': 'INT',
            'demo': 'INT',
            'dispo': 'VARCHAR(50)',
            'job_status': 'VARCHAR(100)',
            'inserted_at': 'TIMESTAMP'
        }
    }
    return silver_data_types

if __name__ == "__main__":
    # Example usage
    schema_names = ['bronze', 'silver']  # Example schema names
    table_names = ['leads_parquet', 'csv_snapshots']  # Example table names

    check_schema_existence(schema_names)
    check_table_existence('bronze', table_names)

    # Fetch column names for a specific table
    connection_uri = get_connection_uri()  # Use the new function to get the connection URI
    columns = get_schema_table_columns(connection_uri, 'bronze', table_names)  # Pass the connection URI here
    print(columns)
    
    # Get data types for bronze tables
    bronze_types = get_bronze_table_data_types()
    print(bronze_types)
    
    # Get data types for silver tables
    silver_types = get_silver_table_data_types()
    print(silver_types)