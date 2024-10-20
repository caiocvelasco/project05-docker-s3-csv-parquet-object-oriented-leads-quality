
# Modifying sys.path to include '/workspace/etl' and '/workspace/etl/utils' in the list of paths
import sys
sys.path.append('/workspace/etl')
sys.path.append('/workspace/etl/utils')
print(sys.path)

# Importing Modules
import logging
from sqlalchemy import text
from sqlalchemy.exc import SQLAlchemyError
from utils_connection import create_db_engine, get_connection_uri
from subprocess import call

# Function to run SQL script using shell command and get connection details from utils_connection.py
def run_sql_script(script_name):
    """
    Execute a SQL script using the connection details from utils_connection.py.

    Args:
        script_name (str): The name of the SQL script file.
    """
    # Get the SQL script path and connection URI
    script_path = f"/workspace/sql_scripts/{script_name}"
    connection_uri = get_connection_uri()  # Get the connection URI from utils_connection

    # Construct the psql command with the connection URI
    command = f"psql {connection_uri} -f {script_path}"

    try:
        # Execute the command
        result = call(command, shell=True)
        if result == 0:
            print(f"SQL script {script_name} executed successfully.")
        else:
            print(f"Failed to execute the script {script_name}.")
        return result
    except Exception as e:
        print(f"An error occurred while running the SQL script: {e}")
        return None

# Function to check schema existence
def check_schema_existence(connection_uri, schema_names):
    try:
        db_engine = create_db_engine(connection_uri)
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
def check_table_existence(connection_uri, schema_name, table_names):
    try:
        db_engine = create_db_engine(connection_uri)
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

# Main block for running the script directly
if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    
    # Ingestion Parameters for Bronze, Silver, and Gold Layers
    schema_names = ['bronze', 'silver', 'gold']
    bronze_schema = 'bronze'
    silver_schema = 'silver'
    gold_schema   = 'gold'

    create_schemas_script_path       = 'schemas/create_schemas.sql'
    create_bronze_tables_script_path = 'bronze/create_bronze_tables.sql'
    create_silver_tables_script_path = 'silver/create_silver_tables.sql'

    # Table names per Schema
    tables_in_bronze = ['leads_parquet', 'csv_snapshots']
    tables_in_silver = ['stg_leads_parquet', 'stg_csv_snapshots']

    # 1) Run create_schemas.sql 
    print("----- Creating SCHEMAS in PostgreSQL -----")
    result = run_sql_script(create_schemas_script_path)
    if result == 0:
        print("Schemas created successfully.")
    else:
        print("Failed to create schemas.")
    
    # 2) Check schema existence
    check_schema_existence(get_connection_uri(), schema_names)

    # 3) Run create_bronze_tables.sql 
    print("----- Creating BRONZE Tables in PostgreSQL -----")
    result = run_sql_script(create_bronze_tables_script_path)
    if result == 0:
        print("Bronze tables created successfully.")
    else:
        print("Failed to create bronze tables.")
    
    # 4) Run create_silver_tables.sql 
    print("----- Creating SILVER Tables in PostgreSQL -----")
    result = run_sql_script(create_silver_tables_script_path)
    if result == 0:
        print("Silver tables created successfully.")
    else:
        print("Failed to create silver tables.")

    # 5) Check table existence for Bronze and Silver
    check_table_existence(get_connection_uri(), bronze_schema, tables_in_bronze)
    check_table_existence(get_connection_uri(), silver_schema, tables_in_silver)