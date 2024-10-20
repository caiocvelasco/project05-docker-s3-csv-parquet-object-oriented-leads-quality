# Modifying sys.path to include '/workspace/etl' and '/workspace/etl/utils' in the list of paths
import sys
sys.path.append('/workspace/etl')
sys.path.append('/workspace/etl/utils')
print(sys.path)

# Importing Modules
import logging
from sqlalchemy import create_engine, text
from sqlalchemy.exc import SQLAlchemyError
from utils_connection import get_connection_uri
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
            logging.info(f"SQL script {script_name} executed successfully.")
        else:
            logging.error(f"Failed to execute the script {script_name}. Return code: {result}")
        return result
    except Exception as e:
        logging.exception(f"An error occurred while running the SQL script: {e}")
        return None

# Main block for running the script directly
if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    
    # Define Silver Schema
    silver_schema = 'silver'
    
    # Define Tables in Silver
    tables_in_silver = ['stg_leads_parquet', 'stg_csv_snapshots']

    # Define path to the Script to Apply Data Types in Silver Tables
    apply_silver_types_script_path = 'silver/apply_silver_types.sql'

    # 1) Run apply_silver_types.sql 
    logging.info("----- Applying Types to Silver Tables in PostgreSQL -----")
    result = run_sql_script(apply_silver_types_script_path)
    if result == 0:
        logging.info("Types have been applied successfully.")
    else:
        logging.error("Failed to apply types.")