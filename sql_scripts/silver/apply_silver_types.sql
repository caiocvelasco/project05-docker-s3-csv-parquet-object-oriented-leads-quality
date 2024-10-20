-- The following ALTER TABLE statements modify the data types of various columns 
-- in the STG_LEADS_PARQUET and STG_CSV_SNAPSHOTS tables. Specifically, for the 
-- STATE column, we are using a CASE statement to replace any occurrences of 
-- '<NA>' with NULL. This ensures that invalid or missing state values are 
-- correctly interpreted as NULL in the PostgreSQL database, allowing for 
-- more accurate data representation and analysis. The same logic is applied 
-- to other relevant tables to maintain consistency across the dataset.


-- Alter table for stg_leads_parquet
ALTER TABLE SILVER.STG_LEADS_PARQUET
    ALTER COLUMN lead_uuid SET DATA TYPE VARCHAR(255),
    ALTER COLUMN phone_hash SET DATA TYPE VARCHAR(255),
    ALTER COLUMN email_hash SET DATA TYPE VARCHAR(255),
    ALTER COLUMN _extraction_date SET DATA TYPE DATE USING _extraction_date::date;

-- Alter table for stg_csv_snapshots
ALTER TABLE SILVER.STG_CSV_SNAPSHOTS
    ALTER COLUMN entry_date SET DATA TYPE DATE USING entry_date::date,
    ALTER COLUMN lead_number SET DATA TYPE INT USING lead_number::integer,
    ALTER COLUMN email_hash SET DATA TYPE VARCHAR(255),
    ALTER COLUMN phone_hash SET DATA TYPE VARCHAR(255),
    ALTER COLUMN city SET DATA TYPE VARCHAR(100),
    ALTER COLUMN state SET DATA TYPE CHAR(2) 
        USING CASE 
            WHEN state = '<NA>' THEN NULL 
            ELSE state 
        END,
    ALTER COLUMN zip SET DATA TYPE VARCHAR(10),
    ALTER COLUMN appt_date SET DATA TYPE TIMESTAMP 
        USING CASE 
            WHEN appt_date = '<NA>' THEN NULL 
            ELSE appt_date::timestamp without time zone 
        END,
    ALTER COLUMN set SET DATA TYPE INT USING set::integer,
    ALTER COLUMN demo SET DATA TYPE INT USING demo::integer,
    ALTER COLUMN dispo SET DATA TYPE VARCHAR(50),
    ALTER COLUMN job_status SET DATA TYPE VARCHAR(100),
    ALTER COLUMN location SET DATA TYPE VARCHAR(255),
    ALTER COLUMN _extraction_date SET DATA TYPE DATE USING _extraction_date::date,
    ALTER COLUMN _partition_date SET DATA TYPE DATE USING _partition_date::date;

-- Alter tables for stg_csv_data_01 to stg_csv_data_22
DO $$
DECLARE
    i INT;
    table_name TEXT;
BEGIN
    FOR i IN 1..22 LOOP
        table_name := format('stg_csv_data_%s', lpad(i::text, 2, '0'));  -- Ensure leading zero
        EXECUTE format('
            ALTER TABLE SILVER.%I
                ALTER COLUMN entry_date SET DATA TYPE DATE USING entry_date::date,
                ALTER COLUMN lead_number SET DATA TYPE INT USING lead_number::integer,
                ALTER COLUMN email_hash SET DATA TYPE VARCHAR(255),
                ALTER COLUMN phone_hash SET DATA TYPE VARCHAR(255),
                ALTER COLUMN city SET DATA TYPE VARCHAR(100),
                ALTER COLUMN state SET DATA TYPE CHAR(2) 
                    USING CASE 
                        WHEN state = ''<NA>'' THEN NULL 
                        ELSE state 
                    END,
                ALTER COLUMN zip SET DATA TYPE VARCHAR(10),
                ALTER COLUMN appt_date SET DATA TYPE TIMESTAMP 
                    USING CASE 
                        WHEN appt_date = ''<NA>'' THEN NULL 
                        ELSE appt_date::timestamp without time zone 
                    END,
                ALTER COLUMN set SET DATA TYPE INT USING set::integer,
                ALTER COLUMN demo SET DATA TYPE INT USING demo::integer,
                ALTER COLUMN dispo SET DATA TYPE VARCHAR(50),
                ALTER COLUMN job_status SET DATA TYPE VARCHAR(100),
                ALTER COLUMN location SET DATA TYPE VARCHAR(255),
                ALTER COLUMN _extraction_date SET DATA TYPE DATE USING _extraction_date::date,
                ALTER COLUMN _partition_date SET DATA TYPE DATE USING _partition_date::date;
        ', table_name);
    END LOOP;
END $$;