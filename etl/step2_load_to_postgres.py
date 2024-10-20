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
from extract import DataExtractor
from transform import DataTransformer
from utils_connection import get_s3_parquet_file_key, get_connection_uri
from utils_checks_db import get_schema_table_columns

class DataLoader:
    def __init__(self):
        self.connection_uri = get_connection_uri()  # Fetch connection URI
        self.engine = create_engine(self.connection_uri)

    def load_parquet_to_postgres(self, parquet_df: pd.DataFrame, table_name: str, schema: str):
        """Loads a Parquet DataFrame into the specified Postgres table with schema validation."""
        try:
            # Validate schema before loading
            schema_table_columns = get_schema_table_columns(self.connection_uri, schema, [table_name])
            schema_columns = schema_table_columns.get(table_name, [])

            if not schema_columns:
                logging.error(f"No columns found for table '{schema}.{table_name}' in schema.")
                return

            # Reorder DataFrame columns to match schema (optional)
            parquet_df = parquet_df[schema_columns]  # Keep only schema columns, discard others

            # Check if DataFrame columns match schema columns
            if all(column in parquet_df.columns for column in schema_columns):
                with self.engine.begin() as conn:
                    parquet_df.to_sql(table_name, conn, schema=schema, if_exists='append', index=False)
                    logging.info(f"Successfully loaded Parquet data to '{schema}.{table_name}'.")
            else:
                logging.error(f"DataFrame columns do not match the schema columns for '{schema}.{table_name}': {schema_columns}")
        
        except SQLAlchemyError as e:
            logging.error(f"SQLAlchemyError while loading Parquet data to '{schema}.{table_name}': {str(e)}")
        except Exception as e:
            logging.error(f"An error occurred: {str(e)}") 

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

    # Table names per Schema
    schema_names = ['bronze', 'silver']
    bronze_table_names = ['leads_parquet', 'csv_snapshots']
    silver_table_names =  ['stg_leads_parquet', 'stg_csv_snapshots']
    
    # Instantiate the DataExtractor, DataLoader, and DataTransformer
    extractor = DataExtractor()
    loader = DataLoader()
    transformer = DataTransformer()

    # Get Parquet file
    parquet_key = get_s3_parquet_file_key()  # Retrieve the Parquet file key
    parquet_df = extractor.extract_parquet(parquet_key)

    # Get all CSV files
    csv_df = extractor.extract_all_csv()

    # Get Bronze Schema
    bronze_schema = schema_names[0]

    # Load data into Bronze in Postgres
    for table_name in bronze_table_names:
        if table_name == 'leads_parquet':
            loader.load_parquet_to_postgres(parquet_df, table_name, bronze_schema)
        elif table_name == 'csv_snapshots':
            loader.load_csv_to_postgres(csv_df, table_name, bronze_schema)

    # Get Silver Schema
    silver_schema = schema_names[1]

    # Get data from Bronze in Postgres and Apply transformations
    for table_name in bronze_table_names:
        if table_name == 'leads_parquet':
            parquet_data = transformer.get_data_from_postgres_to_pd(bronze_schema, 'leads_parquet')
            silver_parquet_data = transformer.clean_parquet(parquet_data)

            # Debugging: Print the columns of the transformed DataFrame
            print("Transformed and Renamed Parquet Data:")
            print(silver_parquet_data.head())
            print("Columns after mapping:", silver_parquet_data.columns.tolist())

        elif table_name == 'csv_snapshots':
            csv_data = transformer.get_data_from_postgres_to_pd(bronze_schema, 'csv_snapshots')
            silver_csv_data = transformer.clean_csv(csv_data)

            # Debugging: Print the columns of the transformed DataFrame
            print("Transformed and Renamed CSV Data:")
            print(silver_csv_data.head())
            print("Columns after mapping:", silver_csv_data.columns.tolist())
    
    # Load data into Silver in Postgres
    for table_name in silver_table_names:
        if table_name == 'stg_leads_parquet':
            print("Initiated Load into Postgres (Silver.stg_leads_parquet):")
            loader.load_parquet_to_postgres(silver_parquet_data, table_name, silver_schema)
        elif table_name == 'stg_csv_snapshots':
            print("Initiated Load into Postgres (Silver.stg_csv_snapshots):")
            loader.load_csv_to_postgres(silver_csv_data, table_name, silver_schema)