-- Note: "CREATE OR REPLACE VIEW" is the correct syntax for PostgreSQL for Views (just in case).
-- Note: Columns names are quoted (e.g.: CustomerID is now "CustomerID"). 
-- -- This prevents errors such as UndefinedColumn when PostgreSQL tries to reference columns.

-- 1) Summary
-- Create tables with raw data from parquet files and CSV files
-- Data was minimally transformed to standardize all the CSV files into a unified format
-- DB: LEADS_DB
-- SCHEMA: SILVER

-- 2) Tables
-- STG_LEADS_PARQUET
-- Purpose: Stores hashed lead data (phone_hash, email_hash) with unique constraints.
-- Key Columns: 
--   lead_UUID (Primary Key, UUID)
--   phone_hash, email_hash (Unique identifiers for leads)
--   inserted_at (Tracks when the record is added, defaulting to current timestamp) 

-- STG_CSV_SNAPSHOTS
-- Purpose: Stores lead snapshot details, linked to LEADS_PARQUET via email_hash and phone_hash.
-- Key Columns:
--   LEADNUMBER (Primary Key, INT)
--   email_hash, phone_hash (Foreign Keys referencing LEADS_PARQUET)
--   inserted_at (Automatically records insertion time with DEFAULT CURRENT_TIMESTAMP)

-- STG_LEADS_PARQUET
CREATE TABLE IF NOT EXISTS SILVER.STG_LEADS_PARQUET (
    "lead_uuid" TEXT,          -- Changed UUID to TEXT
    "phone_hash" TEXT,              -- Changed VARCHAR(255) to TEXT
    "email_hash" TEXT,              -- Changed VARCHAR(255) to TEXT
    "_extraction_date" TEXT
);

-- STG_CSV_SNAPSHOTS
CREATE TABLE IF NOT EXISTS SILVER.STG_CSV_SNAPSHOTS (
    "entry_date" TEXT,                     -- Changed DATE to TEXT
    "lead_number" TEXT,        -- Changed INT to TEXT
    "email_hash" TEXT,                     -- Changed VARCHAR(255) to TEXT
    "phone_hash" TEXT,                     -- Changed VARCHAR(255) to TEXT
    "city" TEXT,                           -- Changed VARCHAR(100) to TEXT
    "state" TEXT,                          -- Changed CHAR(2) to TEXT
    "zip" TEXT,                            -- Changed VARCHAR(10) to TEXT
    "appt_date" TEXT,                      -- Changed TIMESTAMP to TEXT
    "set" TEXT,                      -- Changed INT to TEXT
    "demo" TEXT,                           -- Changed INT to TEXT
    "dispo" TEXT,                          -- Changed VARCHAR(50) to TEXT
    "job_status" TEXT,                     -- Changed VARCHAR(100) to TEXT
    "location" TEXT,
    "_extraction_date" TEXT,   
    "_partition_date" TEXT  
    -- CONSTRAINT fk_email_hash FOREIGN KEY ("email_hash") REFERENCES SILVER.STG_LEADS_PARQUET("email_hash"),
    -- CONSTRAINT fk_phone_hash FOREIGN KEY ("phone_hash") REFERENCES SILVER.STG_LEADS_PARQUET("phone_hash")
);

-- DO block to create tables for each CSV snapshot
DO $$
DECLARE
    i INT;
    padded_number TEXT;  -- Variable to store the padded number
BEGIN
    FOR i IN 1..22 LOOP
        -- Pad the number with leading zeros
        padded_number := LPAD(i::TEXT, 2, '0');
        
        EXECUTE format('
            CREATE TABLE IF NOT EXISTS SILVER.stg_csv_data_%s (
                "entry_date" TEXT,                     -- Changed to TEXT
                "lead_number" TEXT,                    -- Changed to TEXT
                "email_hash" TEXT,                     -- Changed to TEXT
                "phone_hash" TEXT,                     -- Changed to TEXT
                "city" TEXT,                           -- Changed to TEXT
                "state" TEXT,                          -- Changed to TEXT
                "zip" TEXT,                            -- Changed to TEXT
                "appt_date" TEXT,                      -- Changed to TEXT
                "set" TEXT,                            -- Changed to TEXT
                "demo" TEXT,                           -- Changed to TEXT
                "dispo" TEXT,                          -- Changed to TEXT
                "job_status" TEXT,                     -- Changed to TEXT
                "location" TEXT,                       -- Changed to TEXT
                "_extraction_date" TEXT,               -- Changed to TEXT
                "_partition_date" TEXT                  -- Changed to TEXT
            );', padded_number);
    END LOOP;
END $$;