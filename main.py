import subprocess
import logging

# Set up logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

# List of scripts to run in order
scripts = [
    '/workspace/etl/step1_postgres_data_definition.py',
    '/workspace/etl/step2_load_to_postgres.py',
    '/workspace/etl/step3_partition_and_load_all_csv.py',
    '/workspace/etl/step4_data_types_postgres.py',
    '/workspace/etl/step5_create_gold_tables.py',
    '/workspace/etl/step6_insert_into_gold_tables.py'
]

def run_script(script_name):
    """Run a Python script using subprocess."""
    try:
        logging.info(f"Running script: {script_name}")
        subprocess.run(['python', script_name], check=True)
        logging.info(f"Successfully completed: {script_name}")
    except subprocess.CalledProcessError as e:
        logging.error(f"Error occurred while running {script_name}: {e}")

def main():
    """Main function to run all scripts in order."""
    for script in scripts:
        run_script(script)

if __name__ == '__main__':
    main()