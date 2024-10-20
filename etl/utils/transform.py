# Modifying sys.path to include '/workspace/etl' and '/workspace/etl/utils' in the list of paths
import sys
sys.path.append('/workspace/etl')
sys.path.append('/workspace/etl/utils')
print(sys.path)

# Importing Modules
import logging
import pandas as pd
import numpy as np
import re
from sqlalchemy import create_engine
from extract import DataExtractor
# from step2_load_to_postgres import DataLoader # (Check comment on the last part: if __name__ == "__main__":)
from utils_connection import get_s3_parquet_file_key, get_connection_uri

class DataTransformer:
    def __init__(self):
        """Initialize the DataTransform class."""
        self.engine = create_engine(get_connection_uri())

    def get_data_from_postgres_to_pd(self, schema_name: str, table_name: str) -> pd.DataFrame:
        """
        Loads data from a PostgreSQL table in a given schema into a Pandas DataFrame.

        Args:
            schema_name (str): The name of the schema.
            table_name (str): The name of the table to load.

        Returns:
            pd.DataFrame: Data loaded from the specified schema and table.
        """
        query = f"SELECT * FROM {schema_name}.{table_name}"
        try:
            df = pd.read_sql(query, self.engine)
            print(f"Data loaded successfully from {schema_name}.{table_name}")
            return df
        except Exception as e:
            print(f"Error loading data from {schema_name}.{table_name}: {e}")
            return None

    def clean_csv(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Cleans CSV data based on the outlined steps.
        
        1) General cleaning:
        - Drops rows containing "-----" in any column.
        - Replaces 'nan' and 'None' values with NULL (represented by pd.NA).

        2) Column-specific cleaning:
        ( 'Bronze' Column Name -> 'Silver' Column Name: xyz explanation )
        - 'ENTRYDATE' -> 'entry_date': Converts to 'YYYY-MM-DD' format. Invalid dates are set to NULL.
        - 'LEADNUMBER' -> 'lead_number': No cleaning performed.
        - 'email_hash': No cleaning performed.
        - 'phone_hash': No cleaning performed.
        - 'CITY' -> 'city': No cleaning performed.
        - 'STATE' -> 'state': Ensures valid 2-letter state codes. Invalid entries are replaced with NULL or inferred from location data.
        - 'ZIP' -> 'zip': Drops leading '0'. Ensures valid 5-digit integers. Invalid entries are set to NULL.
        - 'APPT_DATE' -> 'appt_date': Converts to 'YYYY-MM-DD'. Rows with 'nu' are dropped, and invalid entries are set to NULL.
        - 'Set' -> 'set': No cleaning performed.
        - 'Demo' -> 'demo': Converts True/False values to '1/0' and stores them as strings.
        - 'Dispo' -> 'dispo': No cleaning performed.
        - 'JOB_STATUS' -> 'job_status': No cleaning performed.
        - 'location': Cleans leading/trailing spaces. Extracts valid state codes if available.
        - '_extraction_date': No cleaning performed.
        - '_partition_date': No cleaning performed.

        3) Post-processing:
        - After all transformations, all columns are converted to string type.

        Returns:
            pd.DataFrame: The cleaned DataFrame with all transformations applied.
        """
        
        # 0) General Cleaning
        # Drop rows with "-----" in any column
        df = df[~df.isin(["-----"]).any(axis=1)]

        # Debugging: Print unique values of _partition_date after general cleaning
        # print("Debugging (General Cleaning):", df["_partition_date"].unique())
        
        # Replace 'nan' values with NULL (using pd.NA)
        df = df.replace('nan', pd.NA)

        # Debugging: Print unique values of _partition_date after general cleaning
        # print("Debugging (General Cleaning):", df["_partition_date"].unique())

        # Replace 'None' values with NULL (using pd.NA)
        df = df.replace('None', pd.NA)

        # Debugging: Print unique values of _partition_date after general cleaning
        # print("Debugging (General Cleaning):", df["_partition_date"].unique())

        # 1) Clean 'ENTRYDATE' (convert to proper datetime format)
        def normalize_entry_date(date_str):
            try:
                # Convert to datetime in YYYY-MM-DD format
                if re.match(r"\d{2}-\d{2}-\d{4}", date_str):  # MM-DD-YYYY or DD-MM-YYYY format
                    date = pd.to_datetime(date_str, dayfirst=True, errors='coerce')
                else:
                    date = pd.to_datetime(date_str, errors='coerce')  # Assuming YYYY-MM-DD or invalid formats
                
                # Return date in YYYY-MM-DD format
                return date.strftime('%Y-%m-%d') if pd.notnull(date) else pd.NaT
            except Exception:
                return pd.NaT  # Return Not a Time for invalid dates

        df.loc[:, 'ENTRYDATE'] = df['ENTRYDATE'].apply(normalize_entry_date)

        # Debugging: Print unique values of _partition_date after general cleaning
        # print("Debugging (ENTRYDATE):", df["_partition_date"].unique())

        # 2) Clean 'APPT_DATE'
        df = df[df['APPT_DATE'] != "nu"]  # Drop rows with 'APPT_DATE' equal to "nu"

        # Debugging: Print unique values of _partition_date after general cleaning
        # print("Debugging (APPT_DATE - part 1):", df["_partition_date"].unique())

        def normalize_appt_date(date_str):
            try:
                date = pd.to_datetime(date_str, errors='coerce')  # Convert to datetime
                # Return date as a string in YYYY-MM-DD format
                return date.strftime('%Y-%m-%d') if pd.notnull(date) else pd.NA  # Change None to pd.NA
            except Exception:
                return pd.NA  # Return pd.NA for invalid dates

        df['APPT_DATE'] = df['APPT_DATE'].apply(normalize_appt_date)

        # Debugging: Print unique values of _partition_date after general cleaning
        # print("Debugging (APPT_DATE - part 2):", df["_partition_date"].unique())

        # 3) Ensure 'STATE' column values are valid
        # Get unique STATE values and sort, while ignoring NaN values

        # Debugging: Print unique values of 'STATE' before cleaning
        print("Debugging (STATE before cleaning):", sorted(df["STATE"].dropna().unique()))
        print("Debugging (STATE before cleaning with NA):", df["STATE"].unique())

        # Create a mask for invalid STATE values ('nan', 'nu', '<NA>') and valid location values (not '<NA')
        invalid_state_mask = df['STATE'].isin(['  ', 'nan', 'nu', '<NA>', pd.NA])
        valid_location_mask = (df['location'] != '<NA>') & (df['location'].notna())

        # Clean up location entries by stripping whitespace
        df['location'] = df['location'].str.strip()

        # Extract state abbreviation from the location using regex
        df['location_abbr'] = df['location'].str.extract(r'\|\s*([A-Z]{2})\s*$')

       # Debugging: Check the extracted abbreviations for the first few rows
        print("Extracted Abbreviations from Location (first 5 rows):")
        print(df[['location', 'location_abbr']].head())

        # Create a mask for valid abbreviations
        valid_abbr_mask = df['location_abbr'].notna() & df['location_abbr'].ne('')

        # Replace invalid STATE values with the state abbreviation from the location_abbr column
        df['STATE'] = np.where(invalid_state_mask & valid_location_mask & valid_abbr_mask, 
                            df['location_abbr'], 
                            df['STATE'])

        # Debugging: Check the STATE column after replacement attempt
        print("STATE after attempted replacement (first 5 rows):")
        print(df[['STATE', 'location_abbr']].head())

        # Replace any remaining invalid STATE values ('nan', 'nu', '<NA>', whitespace) with pd.NA
        df['STATE'] = df['STATE'].replace(['nan', 'nu', '<NA>', '  '], pd.NA)

        # Final debugging: Print unique values of 'STATE' after cleaning
        print("Debugging (STATE after final cleaning):", sorted(df["STATE"].dropna().unique()))
        print("Debugging (STATE after final cleaning with NA):", df["STATE"].unique())

        # Drop the temporary 'location_abbr' column if you no longer need it
        df.drop(columns='location_abbr', inplace=True)

        # Final output for verification
        print("Final DataFrame (first 5 rows):")
        print(df.head())
        

        # 4) Clean 'ZIP' column
        def clean_zip(zip_code):
            try:
                # Check if the zip code is a string of digits and has length 5
                if not zip_code.isdigit() or len(zip_code) != 5:
                    return pd.NA  # Return NULL for invalid ZIP codes
                return int(zip_code)  # Convert valid ZIP codes to integers
            except Exception:
                return pd.NA  # Return NULL if any error occurs

        df['ZIP'] = df['ZIP'].apply(clean_zip)
        # Debugging: Print unique values of _partition_date after general cleaning
        # print("Debugging (ZIP - part 1):", df["_partition_date"].unique())

        # Drop rows where ZIP is 0
        df = df[df['ZIP'] != 0]
        # Debugging: Print unique values of _partition_date after general cleaning
        # print("Debugging (ZIP - part 2):", df["_partition_date"].unique())

        # 5) Convert 'Demo' column values to 0 and 1, then to string
        if 'Demo' in df.columns:
            df['Demo'] = df['Demo'].replace({'True': '1', 'False': '0'})
            df['Demo'] = df['Demo'].astype(str)
        # Debugging: Print unique values of _partition_date after general cleaning
        # print("Debugging (Demo):", df["_partition_date"].unique())

        # 6) Map columns for csv_snapshots
        df.rename(columns={
            'ENTRYDATE': 'entry_date',
            'LEADNUMBER': 'lead_number',
            'email_hash': 'email_hash',
            'phone_hash': 'phone_hash',
            'CITY': 'city',
            'STATE': 'state',
            'ZIP': 'zip',
            'APPT_DATE': 'appt_date',
            'Set': 'set',
            'Demo': 'demo',
            'Dispo': 'dispo',
            'JOB_STATUS': 'job_status',
            'location': 'location',
            '_extraction_date': '_extraction_date',
            '_partition_date': '_partition_date'
        }, inplace=True)
        print("Columns after mapping:", df.columns.tolist())

        # 7) Convert all columns to string type
        df = df.astype(str)

        return df

    def clean_parquet(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Cleans a Parquet DataFrame by performing the following steps:
        
        1. Drops rows with missing values in the first three ID columns.
        2. Normalizes `email_hash` and `phone_hash` columns by converting them to lowercase and stripping whitespace.
        3. Removes duplicate rows based on the first three ID columns.
        4. Identifies and prints duplicates based on the combination of `email_hash` and `phone_hash`.
        5. Identifies and prints duplicates across the first three ID columns.
        6. Renames specific columns for consistency.
        
        Args:
            df (pd.DataFrame): Input DataFrame to be cleaned.
            
        Returns:
            pd.DataFrame: Cleaned DataFrame.
        """
        # 1) Drop rows with NaN values in the ID columns (first three columns)
        df = df.dropna(subset=df.columns[:3])  # Assuming first three columns are IDs

        # 2) Normalize hashed emails and phone numbers
        if 'email_hash' in df.columns:
            df['email_hash'] = df['email_hash'].astype(str).str.lower().str.strip()  # Normalize to lowercase and strip whitespace

        if 'phone_hash' in df.columns:
            df['phone_hash'] = df['phone_hash'].astype(str).str.lower().str.strip()  # Normalize to lowercase and strip whitespace

        # 3) Remove duplicates based on the first three ID columns
        df = df.drop_duplicates(subset=df.columns[:3])  # Drop duplicates based on ID columns

        # 4) Check for duplicates in the combination of email_hash and phone_hash
        if 'email_hash' in df.columns and 'phone_hash' in df.columns:
            email_phone_duplicates = df[df.duplicated(subset=['email_hash', 'phone_hash'], keep=False)]
            if not email_phone_duplicates.empty:
                print(f"Duplicates found based on email_hash and phone_hash:\n{email_phone_duplicates}")

        # 5) Check for duplicates across all three ID columns
        id_duplicates = df[df.duplicated(subset=df.columns[:3], keep=False)]
        if not id_duplicates.empty:
            print(f"Duplicates found across all three ID columns:\n{id_duplicates}")

        # 6) Map columns for leads_parquet
        df.rename(columns={
                'lead_UUID': 'lead_uuid',
                'phone_hash': 'phone_hash',
                'email_hash': 'email_hash',
                '_extraction_date': '_extraction_date'
            }, inplace=True)
        print("Columns after mapping:", df.columns.tolist())

        return df

# If you want to test this file by running it, uncomment this section and 
# uncomment the 'from step2_load_to_postgres import DataLoader' at the beginning

# if __name__ == "__main__":
#     logging.basicConfig(level=logging.INFO)

#     # Table names per Schema
#     schema_names = ['bronze', 'silver']
#     bronze_table_names = ['leads_parquet', 'csv_snapshots']
#     silver_table_names =  ['stg_leads_parquet', 'stg_csv_snapshots']
    
#     # Instantiate the DataExtractor, DataLoader, and DataTransformer
#     extractor = DataExtractor()
#     loader = DataLoader()
#     transformer = DataTransformer()

#     # Process daily Parquet file
#     parquet_key = get_s3_parquet_file_key()  # Retrieve the Parquet file key
#     parquet_df = extractor.extract_parquet(parquet_key)

#     # Process daily CSV file
#     csv_df = extractor.extract_all_csv()

#     # Get Bronze Schema
#     bronze_schema = schema_names[0]

#     # Load data into Postgres using the bronze_table_names
#     for table_name in bronze_table_names:
#         if table_name == 'leads_parquet':
#             loader.load_parquet_to_postgres(parquet_df, table_name, bronze_schema)
#         elif table_name == 'csv_snapshots':
#             loader.load_csv_to_postgres(csv_df, table_name, bronze_schema)

#     # Get Silver Schema
#     silver_schema = schema_names[1]

#     # Load data from the bronze tables and apply transformations using transformer functions
#     for table_name in bronze_table_names:
#         if table_name == 'leads_parquet':
#             parquet_data = transformer.get_data_from_postgres_to_pd(bronze_schema, 'leads_parquet')
#             silver_parquet_data = transformer.clean_parquet(parquet_data)

#             # Debugging: Print the columns of the transformed DataFrame
#             print("Transformed and Renamed Parquet Data:")
#             print(silver_parquet_data.head())

#         elif table_name == 'csv_snapshots':
#             csv_data = transformer.get_data_from_postgres_to_pd(bronze_schema, 'csv_snapshots')
#             print(csv_data["_partition_date"].unique())
#             silver_csv_data = transformer.clean_csv(csv_data)
#             print(silver_csv_data["_partition_date"].unique())

#             # Debugging: Print the columns of the transformed DataFrame
#             print("Transformed and Renamed CSV Data:")
#             print(silver_csv_data.head())