-- ============================================================
-- FILE:    04_ETL_email_events.sql
-- ORDER:   Run after 02_ETL_customers.sql and 03_ETL_email_campaigns.sql
-- PROJECT: E-commerce Marketing ROI & Customer Acquisition
--
-- PURPOSE:
-- Cleans and transforms raw email event data from
-- stg_email_events and loads it into the production
-- email_events table.
--
-- PREREQUISITES:
--   01_Schema.sql must be run first
--   02_ETL_customers.sql must be run first (FK dependency)
--   03_ETL_email_campaigns.sql must be run first (FK dependency)
--
-- RAW SOURCE:  stg_email_events  (144,235 rows)
-- TARGET:      email_events      (85,223 rows after cleaning)
--
-- RECORDS EXCLUDED (58,012 rows):
--   - customer_id not found in production customers table
--   - campaign_id not found in production email_campaigns table
--   Only records with valid FK references on both sides are loaded.
--
-- ISSUES CLEANED:
--   1. sent_status      → Mixed case → Proper case
--   2. opened           → Yes/No/True/False → 1/0
--   3. open_timestamp   → Multiple formats → YYYY-MM-DD
--                         NULL retained where email not opened
--   4. click_timestamp  → Empty strings → NULL
--                         NULL retained where email not clicked
--   5. device_type      → Mixed case → Proper case, empty → NULL
--   6. email_client     → Mixed case → Proper case, empty → NULL
-- ============================================================

USE ecommerce_marketing;

-- Updates are allowed 
SET SQL_SAFE_UPDATES =0;

-- Autocommit turned off
SET autocommit = 0;

-- ============================================================
-- STEP 1: INITIAL DATA EXPLORATION
-- ============================================================
-- Preview raw data
SELECT * FROM stg_email_events;

-- Check for NULL event_id or campaign_id
-- Both are required fields and should not be NULL
SELECT event_id, campaign_id, COUNT(*)
FROM stg_email_events
WHERE event_id IS NULL OR campaign_id IS NULL
GROUP BY event_id, campaign_id;

-- Check distinct customer and email combinations
-- Helps identify if one customer has multiple email addresses
SELECT DISTINCT customer_id, email
FROM stg_email_events;

-- ============================================================
-- STEP 2: CLEAN SENT_STATUS AND EMAIL
-- ============================================================
-- Standardize mixed case to proper case
-- (DELIVERED → Delivered, bounced → Bounced)
UPDATE stg_email_events
SET sent_status = LTRIM(CONCAT(UPPER(LEFT(sent_status,1)), LOWER(SUBSTRING(sent_status, 2))))
WHERE sent_status IS NOT NULL;

UPDATE stg_email_events
SET email = REPLACE(email, ' ', '')
WHERE email LIKE '% %';

COMMIT;

-- ============================================================
-- STEP 3: CLEAN OPENED FLAG AND OPEN_TIMESTAMP
-- ============================================================
-- Check distinct values before standardizing
SELECT opened, COUNT(*)
FROM stg_email_events
GROUP BY opened;

-- Standardize Yes/No/True/False → 1/0
UPDATE stg_email_events
 SET opened = CASE 
  WHEN opened IN ('No', '0', 'False') THEN '0'
  WHEN opened IN('Yes', '1', 'True') THEN '1'
 END
  WHERE opened IS NOT NULL;

-- Standardize open_timestamp to YYYY-MM-DD
-- NULL is valid here — means the email was not opened
UPDATE stg_email_events
 SET open_timestamp = CASE
 -- Already correct format (YYYY-MM-DD)
  WHEN open_timestamp REGEXP '^[0-9]{4}-[0-9]{2}-[0-9]{2}$'
  THEN open_timestamp
-- YYYY/MM/DD
  WHEN open_timestamp REGEXP '^[0-9]{4}/[0-9]{2}/[0-9]{2}$'
  THEN DATE_FORMAT(STR_TO_DATE(open_timestamp, '%Y/%m/%d'), '%Y-%m-%d')
-- DD-MM-YYYY
  WHEN open_timestamp REGEXP '^[0-9]{2}-[0-9]{2}-[0-9]{4}$'
  AND CAST(SUBSTRING(open_timestamp, 1, 2) AS UNSIGNED) > 12
  THEN DATE_FORMAT(STR_TO_DATE(open_timestamp, '%d-%m-%Y'), '%Y-%m-%d')
-- MM/DD/YYYY
   WHEN open_timestamp REGEXP '^[0-9]{2}/[0-9]{2}/[0-9]{4}$'
   AND CAST(SUBSTRING(open_timestamp, 1, 2) AS UNSIGNED) <= 12
   THEN DATE_FORMAT(STR_TO_DATE(open_timestamp, '%m/%d/%Y'), '%Y-%m-%d')
-- MM-DD-YYYY
   WHEN open_timestamp REGEXP '^[0-9]{2}-[0-9]{2}-[0-9]{4}$'
   AND CAST(SUBSTRING(open_timestamp, 1, 2) AS UNSIGNED) <= 12
   THEN DATE_FORMAT(STR_TO_DATE(open_timestamp, '%m-%d-%Y'), '%Y-%m-%d')
-- DD/MM/YYYY
  WHEN open_timestamp REGEXP '^[0-9]{2}/[0-9]{2}/[0-9]{4}$'
  THEN DATE_FORMAT(STR_TO_DATE(open_timestamp,'%d/%m/%Y'), '%Y-%m-%d')
-- Month DD, YYYY (December 25, 2025)
  WHEN open_timestamp REGEXP '^[A-Za-z]+ [0-9]{1,2}, [0-9]{4}$'
  THEN DATE_FORMAT(STR_TO_DATE(open_timestamp, '%M %d, %Y'), '%Y-%m-%d')
-- DD Month YYYY (25 December 2025)
  WHEN open_timestamp REGEXP '^[0-9]{1,2} [A-Za-z]+ [0-9]{4}$'
  THEN DATE_FORMAT(STR_TO_DATE(open_timestamp, '%d %M %Y'), '%Y-%m-%d')
 END;

COMMIT;

-- ============================================================
-- STEP 4: CLEAN CLICKED FLAG AND CLICK_TIMESTAMP
-- ============================================================
-- Check distinct values of clicked flag
SELECT clicked, count(campaign_id)
FROM stg_email_events
GROUP BY clicked;

-- Empty strings → NULL
-- NULL is valid here — means the email was not clicked
UPDATE stg_email_events
SET click_timestamp = NULL
 WHERE click_timestamp IS NULL OR TRIM(click_timestamp) = '';

-- ============================================================
-- STEP 5: CLEAN UNSUBSCRIBED FLAG
-- ============================================================
-- Check distinct values — no update needed if already 0/1
-- Kept as exploration to confirm data quality 
 SELECT unsubscribed, count(campaign_id)
FROM stg_email_events
GROUP BY unsubscribed;

-- ============================================================
-- STEP 6: CLEAN DEVICE_TYPE
-- ============================================================
-- Standardize mixed case to proper case, then empty → NULL
UPDATE stg_email_events
SET device_type = LTRIM(CONCAT(UPPER(LEFT(device_type, 1)), LOWER(SUBSTRING(device_type, 2))))
 WHERE device_type IS NOT NULL;
 
UPDATE stg_email_events
SET device_type = NULL
 WHERE device_type IS NULL OR TRIM(device_type) = '';

-- Verify distinct values after cleaning
SELECT device_type, count(campaign_id)
FROM stg_email_events
GROUP BY device_type;

-- ============================================================
-- STEP 7: CLEAN EMAIL_CLIENT
-- ============================================================
-- Standardize mixed case to proper case, then empty → NULL
UPDATE stg_email_events
SET email_client = LTRIM(CONCAT(UPPER(LEFT(email_client, 1)), LOWER(SUBSTRING(email_client, 2))))
 WHERE email_client IS NOT NULL;
 
UPDATE stg_email_events
SET email_client = NULL
 WHERE email_client IS NULL OR TRIM(email_client) = '';

-- Verify distinct values after cleaning
SELECT email_client, count(campaign_id)
FROM stg_email_events
GROUP BY email_client;

COMMIT;

-- ============================================================
-- STEP 8: LOAD INTO PRODUCTION TABLE
-- ============================================================
-- Safe insert: only load records where both FK references
-- exist in production tables (customers and email_campaigns).
-- Records with orphan FKs are excluded rather than nulled
-- because both campaign_id and customer_id are NOT NULL
-- in the production email_events table.
INSERT INTO email_events (
    event_id, 
    campaign_id, 
    customer_id, 
    email, 
    sent_status, 
    opened, 
    open_timestamp, 
    clicked, 
    click_timestamp, 
    unsubscribed, 
    device_type, 
    email_client
)
SELECT 
    ee.event_id, 
    ee.campaign_id, 
    ee.customer_id, 
    ee.email, 
    ee.sent_status, 
    ee.opened, 
    ee.open_timestamp, 
    ee.clicked, 
    ee.click_timestamp, 
    ee.unsubscribed, 
    ee.device_type, 
    ee.email_client
FROM stg_email_events ee
WHERE ee.campaign_id IN (SELECT campaign_id FROM email_campaigns)
  AND ee.customer_id IN (SELECT customer_id FROM customers);
  
COMMIT;

-- ============================================================
-- STEP 9: VERIFY LOAD
-- ============================================================
-- Confirm production and staging row counts across all
-- tables loaded so far in the pipeline
SELECT 'customers' AS table_name, COUNT(*) AS row_count FROM customers
UNION ALL
SELECT 'email_campaigns' AS table_name, COUNT(*) AS row_count FROM email_campaigns
UNION ALL
SELECT 'email_events' AS table_name, COUNT(*) AS row_count FROM email_events
UNION ALL
SELECT 'stg_customers' AS table_name, COUNT(*) AS row_count FROM stg_customers
UNION ALL
SELECT 'stg_email_campaigns' AS table_name, COUNT(*) AS row_count FROM stg_email_campaigns
UNION ALL
SELECT 'stg_email_events' AS table_name, COUNT(*) AS row_count FROM stg_email_events;

