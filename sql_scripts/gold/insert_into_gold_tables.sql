-- email_match: This column is set to TRUE if there is a match based on email_hash 
-- (i.e., if parquet.email_hash is not NULL); otherwise, it's set to FALSE.
-- phone_match: Similar logic is applied for phone_match, where it is TRUE if there 
-- is a match based on phone_hash (i.e., if parquet.phone_hash is not NULL); otherwise, it's set to FALSE

INSERT INTO gold.lead_quality_matching (
    lead_uuid,
    lead_number,
    email_hash,
    phone_hash,
    city,
    state,
    zip,
    appt_date,
    set,
    demo,
    dispo,
    job_status,
    location,
    appointment_scheduled,
    demo_scheduled,
    email_match,         
    phone_match,         
    _extraction_date,
    _partition_date,
    conversion_rate,     
    lead_quality_flag
)
SELECT 
    parquet.lead_uuid,
    csv.lead_number,
    csv.email_hash,
    csv.phone_hash,
    csv.city,
    csv.state,
    csv.zip,
    csv.appt_date,
    csv.set,
    csv.demo,
    csv.dispo,
    csv.job_status,
    csv.location,
    CASE WHEN csv.set = 1 THEN TRUE ELSE FALSE END AS appointment_scheduled,
    CASE WHEN csv.demo = 1 THEN TRUE ELSE FALSE END AS demo_scheduled,
    CASE 
        WHEN parquet.email_hash IS NOT NULL THEN TRUE 
        ELSE FALSE 
    END AS email_match,  -- Logic for email match
    CASE 
        WHEN parquet.phone_hash IS NOT NULL THEN TRUE 
        ELSE FALSE 
    END AS phone_match,  -- Logic for phone match
    csv._extraction_date,
    csv._partition_date,
    NULL AS conversion_rate,  -- Placeholder for conversion rate calculation
    CASE 
        WHEN csv.set = 1 AND csv.demo = 1 THEN 'High Quality'
        WHEN csv.set = 1 THEN 'Medium Quality'
        ELSE 'Low Quality'
    END AS lead_quality_flag
FROM silver.stg_csv_snapshots AS csv
LEFT JOIN silver.stg_leads_parquet AS parquet 
ON csv.email_hash = parquet.email_hash OR csv.phone_hash = parquet.phone_hash;