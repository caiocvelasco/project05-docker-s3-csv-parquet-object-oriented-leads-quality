CREATE TABLE IF NOT EXISTS gold.lead_quality_matching (
    lead_uuid VARCHAR(255),
    lead_number INT,
    email_hash VARCHAR(255),
    phone_hash VARCHAR(255),
    city VARCHAR(100),
    state CHAR(2),
    zip VARCHAR(10),
    appt_date TIMESTAMP,
    set INT,
    demo INT,
    dispo VARCHAR(50),
    job_status VARCHAR(100),
    location VARCHAR(255),
    appointment_scheduled BOOLEAN,
    demo_scheduled BOOLEAN,
    email_match BOOLEAN,  -- New column to indicate email match
    phone_match BOOLEAN,  -- New column to indicate phone match
    _extraction_date DATE,
    _partition_date DATE,
    conversion_rate FLOAT,
    lead_quality_flag VARCHAR(50)
);