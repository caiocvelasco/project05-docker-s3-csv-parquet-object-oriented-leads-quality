

-- 1) Summary
-- Create tables with raw data from parquet files and CSV files
-- Data was minimally transformed to standardize all the CSV files into a unified format
-- DB: LEADS_DB
-- SCHEMA: BRONZE

-- 2) Tables
-- LEADS_PARQUET
-- Purpose: Stores hashed lead data (phone_hash, email_hash) with unique constraints.
-- Key Columns: 
--   lead_UUID (Primary Key, UUID)
--   phone_hash, email_hash (Unique identifiers for leads)
--   inserted_at (Tracks when the record is added, defaulting to current timestamp) 

-- CSV_SNAPSHOTS
-- Purpose: Stores lead snapshot details, linked to LEADS_PARQUET via email_hash and phone_hash.
-- Key Columns:
--   LEADNUMBER (Primary Key, INT)
--   email_hash, phone_hash (Foreign Keys referencing LEADS_PARQUET)
--   inserted_at (Automatically records insertion time with DEFAULT CURRENT_TIMESTAMP)

-- LEADS_PARQUET
CREATE TABLE IF NOT EXISTS BRONZE.LEADS_PARQUET (
    "lead_UUID" TEXT,
    "phone_hash" TEXT,
    "email_hash" TEXT,
    "_extraction_date" TEXT
);

-- CSV_SNAPSHOTS
CREATE TABLE IF NOT EXISTS BRONZE.CSV_SNAPSHOTS (
    "ENTRYDATE" TEXT,          
    "LEADNUMBER" TEXT,  
    "email_hash" TEXT,         
    "phone_hash" TEXT,         
    "CITY" TEXT,               
    "STATE" TEXT,              
    "ZIP" TEXT,                
    "APPT_DATE" TEXT,          
    "Set" TEXT,                
    "Demo" TEXT,               
    "Dispo" TEXT,              
    "JOB_STATUS" TEXT,         
    "location" TEXT,           
    "_extraction_date" TEXT,   
    "_partition_date" TEXT      
    -- CONSTRAINT fk_email_hash FOREIGN KEY ("email_hash") REFERENCES BRONZE.LEADS_PARQUET("email_hash"),
    -- CONSTRAINT fk_phone_hash FOREIGN KEY ("phone_hash") REFERENCES BRONZE.LEADS_PARQUET("phone_hash")
);