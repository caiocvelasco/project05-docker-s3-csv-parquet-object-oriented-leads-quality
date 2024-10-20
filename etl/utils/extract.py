# Modifying sys.path to include '/workspace/etl' and '/workspace/etl/utils' in the list of paths
import sys
sys.path.append('/workspace/etl')
sys.path.append('/workspace/etl/utils')
print(sys.path)

# Importing Modules
import os 
import io
import logging
from datetime import datetime
import pandas as pd
from utils_connection import create_s3_client, get_s3_bucket_name, get_sftp_files_prefix, get_s3_parquet_file_key

class DataExtractor:
    def __init__(self):
        """Initialize parameters."""
        try:
            self.bucket_name = get_s3_bucket_name()  # Use the utility function to get bucket name
            self.sftp_prefix = get_sftp_files_prefix()  # Use the utility function for SFTP prefix
            print(f"Using bucket: {self.bucket_name}, Prefix: {self.sftp_prefix}")
            self.s3_client = create_s3_client()  # Create S3 client here
        except Exception as e:
            logging.error(f"Error initializing DataExtractor: {e}")
            raise

    def get_parquet_from_s3_to_pd(self, parquet_key: str) -> tuple[pd.DataFrame, list, tuple, str]:
        """Fetch a parquet file from the S3 bucket."""
        try:
            parquet_obj = self.s3_client.get_object(Bucket=self.bucket_name, Key=parquet_key)
            parquet_buffer = io.BytesIO(parquet_obj['Body'].read())
            parquet_df = pd.read_parquet(parquet_buffer)

            # Get columns and shape for additional checks/logging
            columns = parquet_df.columns.tolist()
            shape = parquet_df.shape
            file_name = os.path.basename(parquet_key)

            logging.info(f"Successfully loaded parquet file: {file_name} with columns: {columns} and shape: {shape}")
            return parquet_df, columns, shape, file_name
        except Exception as e:
            logging.error(f"Error loading parquet file: {parquet_key}. Error: {e}")
            raise
    
    def load_csv_from_s3_to_pd(self, file_key: str) -> pd.DataFrame:
        """Load a single CSV file from S3 into a Pandas DataFrame."""
        try:
            csv_obj = self.s3_client.get_object(Bucket=self.bucket_name, Key=file_key)
            df = pd.read_csv(csv_obj['Body'])
            logging.info(f"Successfully loaded CSV file: {file_key} with shape: {df.shape}")
            return df
        except Exception as e:
            logging.error(f"Error loading CSV file: {file_key}. Error: {e}")
            raise

    def minimal_clean_csv(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Clean and standardize the DataFrame by performing minimal transformations after extraction:
        - Renames columns to standardized names (e.g., 'CityName' to 'CITY', 'Appt Date' to 'APPT_DATE', etc).
        - Adds a 'location' column with NULL values if it does not exist so that all CSVs look the same.
        - Logs the changes and data types of the cleaned DataFrame.

        Args:
            df (pd.DataFrame): Input DataFrame to be cleaned.

        Returns:
            pd.DataFrame: Cleaned and standardized DataFrame.
        """
        # Standardize column names
        rename_map = {
            'CityName': 'CITY',  # Example of a discrepancy
            'Appt Date': 'APPT_DATE',
            'Job Status': 'JOB_STATUS'
        }
        df.rename(columns=rename_map, inplace=True)

        # Add 'location' column with NULL values if it does not exist
        if 'location' not in df.columns:
            df['location'] = pd.NA  # Use pd.NA for nullable integers, floats, etc        

        logging.info("DataFrame cleaned and standardized.")
        logging.debug(f"Data types after cleaning: {df.dtypes}")

        return df
    
    def add_extraction_date(self, df: pd.DataFrame, extraction_date: str) -> pd.DataFrame:
        """Add a _extraction_date column to the DataFrame."""        
        df['_extraction_date'] = extraction_date
        logging.info(f"Extraction date {extraction_date} added to DataFrame.")
        return df
    
    def add_partition_date(self, df: pd.DataFrame, partition_date: str) -> pd.DataFrame:
        """Add a _partition_date column to the DataFrame."""        
        df['_partition_date'] = partition_date
        logging.info(f"Partition date {partition_date} added to DataFrame.")
        return df

    def extract_all_csv(self) -> pd.DataFrame:
        """
        Extract all relevant CSV files from S3, clean the data, and add partition and extraction dates.

        The function processes multiple CSV files from S3 by:
        - Loading each CSV into a Pandas DataFrame (This DataFrame will contain all CSVs, partitioned by '_partition_date').
        - Cleaning and standardizing the data using `minimal_clean_csv` function.
        - Adding an extraction date for tracking when the data was pulled.
        - Adding a partition date based on the file index to identify each CSV in the DataFrame.

        Returns:
            A concatenated DataFrame containing all processed CSV files partitioned by '_partition_date'.
            If any errors occur, an empty DataFrame is returned.
        """
        try:
            # Define the range of partition dates
            start_date = datetime(2024, 10, 1)
            end_date = datetime(2024, 10, 22)
            date_range = pd.date_range(start=start_date, end=end_date)
            partition_dates = [date.strftime('%Y-%m-%d') for date in date_range]

            # List all CSV files to be processed
            csv_files = [f"{self.sftp_prefix}{i}.csv" for i in range(1, 23)]
            all_dfs = []

            for i, file_key in enumerate(csv_files):
                df = self.load_csv_from_s3_to_pd(file_key)
                if df.empty:
                    logging.warning(f"No data found for {file_key}. Skipping...")
                    continue
                
                df = self.minimal_clean_csv(df)

                # Add extraction date
                today = datetime.today().strftime('%Y-%m-%d')
                df = self.add_extraction_date(df, today)

                # Add partition date corresponding to the current file index
                partition_date = partition_dates[i] if i < len(partition_dates) else None
                if partition_date:
                    df = self.add_partition_date(df, partition_date)

                all_dfs.append(df)

            # Concatenate all DataFrames into a single DataFrame
            final_df = pd.concat(all_dfs, ignore_index=True)
            # Convert all columns to string
            final_df = final_df.astype(str)
            logging.info(f"Extracted {len(all_dfs)} DataFrames.")
            return final_df

        except Exception as e:
            logging.error(f"Error extracting all CSV files: {e}")
            return pd.DataFrame()  # Return empty DataFrame if extraction fails

    def extract_parquet(self, parquet_key: str) -> pd.DataFrame:
        """
        Extract and clean a Parquet file from S3, adding an extraction date.

        The function retrieves a Parquet file from S3 based on the provided key, 
        adds an extraction date column, and converts all columns to strings.

        Args:
            parquet_key (str): The S3 key for the Parquet file to be processed.

        Returns:
            A Pandas DataFrame containing the processed Parquet file.
            If any errors occur, an empty DataFrame is returned.
        """        
        try:
            parquet_df, _, _, _ = self.get_parquet_from_s3_to_pd(parquet_key)  # Pass the known Parquet key here
            today = datetime.today().strftime('%Y-%m-%d')
            parquet_df = self.add_extraction_date(parquet_df, today)
            # Convert all columns to string
            parquet_df = parquet_df.astype(str)
            return parquet_df
        except Exception as e:
            logging.error(f"Error processing daily Parquet file. Error: {e}")
            return pd.DataFrame()  # Return empty DataFrame if processing fails

if __name__ == "__main__":
    extractor = DataExtractor()

    # Extract all CSV files
    extracted_csv_df = extractor.extract_all_csv()
    print("Extracted All CSV Data:")
    print(extracted_csv_df.head())  # Display combined DataFrame of all CSVs
    unique_partition_dates = extracted_csv_df['_partition_date'].unique()  # Print unique values for _partition_date
    print("CHecking the unique Partition Dates:")
    print(unique_partition_dates)

    # Extract the Parquet file
    parquet_key = get_s3_parquet_file_key()  # Retrieve the Parquet file key
    extracted_parquet_df = extractor.extract_parquet(parquet_key)
    print("Extracted Parquet Data:")
    print(extracted_parquet_df.head())  # Display Parquet Data 