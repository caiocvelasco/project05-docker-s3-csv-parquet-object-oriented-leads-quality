# Modifying sys.path to include '/workspace/etl' and '/workspace/etl/utils' in the list of paths
import sys
sys.path.append('/workspace/etl')
sys.path.append('/workspace/etl/utils')
print(sys.path)

# Importing Modules
import logging
import pandas as pd
from sqlalchemy import create_engine
from sqlalchemy.exc import SQLAlchemyError
from utils_connection import get_connection_uri
from utils_checks_db import get_schema_table_columns

class DataLoader:
    def __init__(self):
        self.connection_uri = get_connection_uri()  # Fetch connection URI
        self.engine = create_engine(self.connection_uri)

    def load_csv_to_postgres(self, csv_df: pd.DataFrame, table_name: str, schema: str):
        """Loads a CSV DataFrame into the specified Postgres table with schema validation."""
        try:
            # Validate schema before loading
            schema_table_columns = get_schema_table_columns(self.connection_uri, schema, [table_name])
            schema_columns = schema_table_columns.get(table_name, [])

            if not schema_columns:
                logging.error(f"No columns found for table '{schema}.{table_name}' in schema.")
                return

            # Reorder DataFrame columns to match schema (optional)
            csv_df = csv_df[schema_columns]  # Keep only schema columns, discard others

            # Convert all columns to string before loading
            csv_df = csv_df.astype(str)

            # Check if DataFrame columns match schema columns
            if all(column in csv_df.columns for column in schema_columns):
                with self.engine.begin() as conn:
                    csv_df.to_sql(table_name, conn, schema=schema, if_exists='append', index=False)
                    logging.info(f"Successfully loaded CSV data to '{schema}.{table_name}'.")
            else:
                logging.error(f"DataFrame columns do not match the schema columns for '{schema}.{table_name}': {schema_columns}")

        except SQLAlchemyError as e:
            logging.error(f"SQLAlchemyError while loading CSV data to '{schema}.{table_name}': {str(e)}")
        except Exception as e:
            logging.error(f"An error occurred: {str(e)}")

# Main block for running the script directly
if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)

    # Define Silver Schema and CSV Table
    silver_schema = 'silver'
    source_table_name = 'stg_csv_snapshots'  # The silver table with cleaned data

    # Instantiate the DataLoader
    loader = DataLoader()

    # Load all data from the silver table into a DataFrame
    with loader.engine.connect() as conn:
        query = f"SELECT * FROM {silver_schema}.{source_table_name}"
        csv_df = pd.read_sql(query, conn)

    # Define the range of partition dates as strings
    start_date = "2024-10-01"
    end_date = "2024-10-22"
    partition_dates = pd.date_range(start=start_date, end=end_date).date.astype(str)  # Create a list of string dates

    # Load each partition date into separate tables
    for i, partition_date in enumerate(partition_dates, start=1):  # Start enumeration from 1
        partition_df = csv_df[csv_df['_partition_date'] == partition_date]  # Filter for the specific partition

        # Define the new table name for this partition with leading zero
        table_name = f"stg_csv_data_{i:02}"  # This ensures leading zero is added

        # Load each partition DataFrame into its respective table
        if not partition_df.empty:  # Only load if the partition DataFrame is not empty
            logging.info(f"Loading data into '{silver_schema}.{table_name}' for date '{partition_date}'...")
            loader.load_csv_to_postgres(partition_df, table_name, silver_schema)
        else:
            logging.warning(f"No data found for date '{partition_date}', skipping '{silver_schema}.{table_name}'.")