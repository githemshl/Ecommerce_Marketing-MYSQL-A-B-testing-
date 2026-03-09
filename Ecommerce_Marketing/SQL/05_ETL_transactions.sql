-- ============================================================
-- FILE:    05_ETL_transactions.sql
-- ORDER:   Run after 02_ETL_customers.sql and 03_ETL_email_campaigns.sql
-- PROJECT: E-commerce Marketing ROI & Customer Acquisition
--
-- PURPOSE:
-- Cleans and transforms raw transaction data from
-- stg_transactions and loads it into the production
-- transactions table.
--
-- PREREQUISITES:
--   01_Schema.sql must be run first
--   02_ETL_customers.sql must be run first (FK dependency)
--   03_ETL_email_campaigns.sql must be run first (FK dependency)
--
-- RAW SOURCE:  stg_transactions  (15,000 rows)
-- TARGET:      transactions      (8,892 rows after cleaning)
--
-- RECORDS EXCLUDED (6,108 rows):
--   - NULL transaction_id
--   - NULL customer_id
--   - customer_id with no matching record in customers table
--
-- ISSUES CLEANED:
--   1. transaction_date  → Multiple formats → YYYY-MM-DD
--   2. revenue           → Empty/text/negative → NULL
--   3. quantity          → Empty/text/zero/negative → NULL
--                          Decimal format (2.0) → integer (2)
--   4. discount_applied  → Empty/text/zero/negative → NULL
--   5. product_category  → Mixed case → Proper case, empty → NULL
--   6. payment_method    → Mixed case → Proper case, empty → NULL
--   7. order_status      → Mixed case → Proper case, empty → NULL
--   8. source_channel    → Mixed case → Proper case, empty → NULL
--   9. campaign_id       → Decimal format (1001.0) → integer (1001)
--                          Empty string → NULL
--                          NULL retained for non-email transactions
--
-- NOTE ON campaign_id:
--   campaign_id is intentionally nullable in production.
--   NULL means the transaction came from a non-email channel
--   (organic, paid search, direct etc.) — not missing data.
--   Raw data stored campaign_id as decimal (1001.0) which was
--   cast to integer using FLOOR() during load.
-- ============================================================

USE ecommerce_marketing;

-- Autocommit turned off
SET autocommit  = 0;

-- Updates are allowed 
SET SQL_SAFE_UPDATES = 0;

-- ============================================================
-- STEP 1: INITIAL DATA EXPLORATION
-- ============================================================
-- Preview raw data
SELECT * FROM stg_transactions;

-- Check pipeline state — verify parent tables are loaded
-- before attempting FK-dependent inserts
SELECT 'customers' AS table_name, COUNT(*) AS row_no FROM customers
UNION ALL SELECT 'email_campaigns', COUNT(*) AS row_no FROM email_campaigns
UNION ALL SELECT 'email_events', COUNT(*) AS row_no FROM email_events
UNION ALL SELECT 'transactions', COUNT(*) AS row_no FROM transactions
UNION ALL SELECT 'marketing_spend', COUNT(*) AS row_no FROM marketing_spend
UNION ALL SELECT 'website_sessions', COUNT(*) AS row_no FROM website_sessions;

-- Check for NULL transaction_id or customer_id
-- Both are required fields and cannot be NULL in production
SELECT  COUNT(*)
FROM stg_transactions
 WHERE transaction_id IS NULL OR TRIM(transaction_id) = '' OR customer_id IS NULL OR TRIM(customer_id) = '';

-- ============================================================
-- STEP 2: CLEAN TRANSACTION_DATE
-- ============================================================
-- Standardize multiple date formats to YYYY-MM-DD
UPDATE stg_transactions
 SET transaction_date = CASE
-- Already correct format (YYYY-MM-DD)
 WHEN transaction_date REGEXP '^[0-9]{4}-[0-9]{2}-[0-9]{2}$'
 THEN transaction_date
-- YYYY/MM/DD
 WHEN transaction_date REGEXP '^[0-9]{4}/[0-9]{2}/[0-9]{2}$'
 THEN DATE_FORMAT(STR_TO_DATE(transaction_date, '%Y/%m/%d'), '%Y-%m-%d')
 -- DD-MM-YYYY
 WHEN transaction_date REGEXP '^[0-9]{2}-[0-9]{2}-[0-9]{4}$'
  AND CAST(SUBSTRING(transaction_date, 1,2) AS UNSIGNED) > 12
 THEN DATE_FORMAT(STR_TO_DATE(transaction_date, '%d-%m-%Y'), '%Y-%m-%d')
-- MM-DD-YYYY
 WHEN transaction_date REGEXP '^[0-9]{2}-[0-9]{2}-[0-9]{4}$'
  AND CAST(SUBSTRING(transaction_date, 1,2) AS UNSIGNED) <= 12
 THEN DATE_FORMAT(STR_TO_DATE(transaction_date, '%m-%d-%Y'), '%Y-%m-%d')
-- MM/DD/YYYY
 WHEN transaction_date REGEXP '^[0-9]{2}/[0-9]{2}/[0-9]{4}$'
  AND CAST(SUBSTRING(transaction_date, 1,2) AS UNSIGNED) <= 12
 THEN DATE_FORMAT(STR_TO_DATE(transaction_date, '%m/%d/%Y'), '%Y-%m-%d')
-- DD/MM/YYYY
 WHEN transaction_date REGEXP '^[0-9]{2}/[0-9]{2}/[0-9]{4}$'
 THEN DATE_FORMAT(STR_TO_DATE(transaction_date, '%d/%m/%Y'), '%Y-%m-%d')
   END;

COMMIT;

-- ============================================================
-- STEP 3: CLEAN NUMERIC COLUMNS (revenue, quantity, discount)
-- ============================================================

-- REVENUE
-- Step 1: Empty strings → NULL
UPDATE stg_transactions 
SET revenue = NULL
 WHERE TRIM(revenue) = '' OR revenue = '';

-- Step 2: Non-numeric values → NULL
UPDATE stg_transactions 
SET revenue = NULL
 WHERE revenue IS NOT NULL 
   AND revenue NOT REGEXP '^-?[0-9]+(\\.[0-9]+)?$';

-- Step 3: Negative values → NULL (revenue cannot be negative)
UPDATE stg_transactions
SET revenue = NULL
 WHERE revenue IS NOT NULL
  AND CAST(revenue AS DECIMAL(10,2)) < 0;

-- Investigate remaining problematic patterns before committing
SELECT revenue, COUNT(*) AS cnt
FROM stg_transactions
 WHERE revenue IS NOT NULL
  AND (
    revenue = ''
    OR revenue REGEXP '[A-Za-z]'
    OR revenue REGEXP '[^0-9.\\-]'
    OR revenue LIKE '%.%.%'
    OR revenue LIKE '%$%'
    OR revenue LIKE '%,%'
    OR revenue LIKE '% %'
    OR revenue IN ('N/A','NA','NULL','null','None','-','#N/A')
  )
GROUP BY revenue
ORDER BY cnt DESC;

COMMIT;

-- QUANTITY
-- Empty strings → NULL, then non-numeric → NULL,
-- then zero/negative → NULL (quantity must be at least 1)
-- Decimal format (2.0) will be cast to integer on load
UPDATE stg_transactions 
SET quantity = NULL
 WHERE TRIM(quantity) = '' OR quantity = '';

UPDATE stg_transactions 
SET quantity = NULL
 WHERE quantity IS NOT NULL
  AND quantity NOT REGEXP '^-?[0-9]+(\\.[0-9]+)?$';
 
DESCRIBE stg_transactions;
DESCRIBE transactions;
 
UPDATE stg_transactions 
SET quantity = NULL
 WHERE quantity IS NOT NULL
  AND CAST(quantity AS DECIMAL(10,1)) <= 0;

-- DISCOUNT APPLIED
-- NULL means no discount was applied — valid business data
UPDATE stg_transactions 
SET discount_applied = NULL
 WHERE TRIM(discount_applied ) = '' OR discount_applied = '';

UPDATE stg_transactions
SET discount_applied = NULL
 WHERE discount_applied IS NOT NULL
  AND discount_applied NOT REGEXP '^-?[0-9]+(\\.[0-9]+)?$';
 
UPDATE stg_transactions
SET discount_applied = NULL
 WHERE discount_applied IS NOT NULL
  AND CAST(discount_applied AS DECIMAL(5,2)) <=0;

-- ============================================================
-- STEP 4: CLEAN TEXT COLUMNS
-- ============================================================
-- Standardize mixed case to proper case, empty → NULL
-- Applies to: product_category, payment_method,
--             order_status, source_channel

-- product_category
UPDATE stg_transactions
SET product_category = LTRIM(CONCAT(UPPER(LEFT(product_category, 1)), LOWER(SUBSTRING(product_category, 2))))
 WHERE product_category IS NOT NULL;

UPDATE stg_transactions 
SET product_category = NULL
 WHERE TRIM(product_category) = '' OR product_category = '';

-- payment_method
UPDATE stg_transactions
SET payment_method = LTRIM(CONCAT(UPPER(LEFT(payment_method, 1)), LOWER(SUBSTRING(payment_method, 2))))
 WHERE payment_method IS NOT NULL;

UPDATE stg_transactions 
SET payment_method = NULL
 WHERE TRIM(payment_method) = '' OR payment_method = '';

-- order_status
UPDATE stg_transactions
SET order_status = LTRIM(CONCAT(UPPER(LEFT(order_status, 1)), LOWER(SUBSTRING(order_status, 2))))
 WHERE order_status IS NOT NULL;

UPDATE stg_transactions 
SET order_status = NULL
 WHERE TRIM(order_status) = '' OR order_status = '';

-- source_channel
UPDATE stg_transactions
SET source_channel = LTRIM(CONCAT(UPPER(LEFT(source_channel, 1)), LOWER(SUBSTRING(source_channel, 2))))
 WHERE source_channel IS NOT NULL;

UPDATE stg_transactions 
SET source_channel = NULL
 WHERE TRIM(source_channel) = '' OR source_channel = '';

COMMIT;

-- ============================================================
-- STEP 5: CLEAN CAMPAIGN_ID
-- ============================================================
-- Raw data stores campaign_id as decimal (1001.0 → 1001)
-- Empty strings → NULL
-- NULL retained for non-email transactions (intentional)

-- Empty strings → NULL
UPDATE stg_transactions
SET campaign_id = NULL
 WHERE campaign_id = '' OR TRIM(campaign_id) = '';

-- Check how many transactions have a campaign_id vs not
-- Helps confirm the email vs non-email transaction split
SELECT 
    SUM(CASE WHEN campaign_id IS NOT NULL AND campaign_id != '' THEN 1 ELSE 0 END) AS has_campaign,
    SUM(CASE WHEN campaign_id IS NULL OR campaign_id = '' THEN 1 ELSE 0 END) AS no_campaign,
    COUNT(*) AS total
FROM stg_transactions;

-- Check orphan campaign_ids (exist in staging but not in email_campaigns)
SELECT DISTINCT c.campaign_id AS orphan_fk, COUNT(*) AS cnt
FROM stg_transactions c
LEFT JOIN email_campaigns p ON c.campaign_id = p.campaign_id
WHERE c.campaign_id IS NOT NULL
  AND c.campaign_id != ''
  AND p.campaign_id IS NULL
GROUP BY c.campaign_id
ORDER BY cnt DESC;

-- Check orphan customer_ids (exist in staging but not in customers)
SELECT DISTINCT c.customer_id AS orphan_fk, COUNT(*) AS cnt
FROM stg_transactions c
LEFT JOIN customers p ON c.customer_id = p.customer_id
WHERE c.customer_id IS NOT NULL
  AND c.customer_id != ''
  AND p.customer_id IS NULL
GROUP BY c.customer_id
ORDER BY cnt DESC;

-- ============================================================
-- STEP 6: VALIDATE BEFORE LOADING
-- ============================================================
-- Confirm staging table schema matches production expectations
DESCRIBE stg_transactions;
DESCRIBE transactions;

-- Check FK constraints on production table
SHOW CREATE TABLE transactions;

-- ============================================================
-- STEP 7: LOAD INTO PRODUCTION TABLE
-- ============================================================
-- NOTE ON CAMPAIGN_ID HANDLING:
--   Raw campaign_id is stored as decimal string ('1001.0').
--   FLOOR(CAST()) converts 1001.0 → 1001 safely.
--   NULL and empty strings are excluded via CASE statement.
--   The FK to email_campaigns allows NULL so non-email
--   transactions load without constraint violations.
--
-- NOTE ON INNER JOIN:
--   INNER JOIN to customers ensures only transactions with
--   a valid customer_id are loaded. Orphan records are excluded.


INSERT INTO transactions (
    transaction_id,
    customer_id,
    transaction_date,
    revenue,
    quantity,
    product_category,
    discount_applied,
    payment_method,
    order_status,
    source_channel,
    campaign_id
)
SELECT
    CAST(t.transaction_id AS UNSIGNED),
    CAST(t.customer_id AS UNSIGNED),
    CAST(t.transaction_date AS DATE),
    CAST(t.revenue AS DECIMAL(10,2)),
    FLOOR(CAST(t.quantity AS DECIMAL(10,1))),
    t.product_category,
    CAST(t.discount_applied AS DECIMAL(5,2)),
    t.payment_method,
    t.order_status,
    t.source_channel,
    CASE
        WHEN t.campaign_id IS NULL
          OR TRIM(t.campaign_id) = ''           THEN NULL
        WHEN t.campaign_id REGEXP '^[0-9]+(\\.[0-9]+)?$'
        THEN CAST(FLOOR(CAST(t.campaign_id AS DECIMAL(10,1))) AS UNSIGNED)
        ELSE NULL
    END
FROM stg_transactions t
INNER JOIN customers c
        ON CAST(t.customer_id AS UNSIGNED) = c.customer_id
WHERE t.transaction_id IS NOT NULL
  AND t.customer_id    IS NOT NULL;

COMMIT;

-- ============================================================
-- STEP 8: VERIFY LOAD
-- ============================================================
SELECT
    (SELECT COUNT(*) FROM transactions)     AS production_rows,
    (SELECT COUNT(*) FROM stg_transactions
      WHERE transaction_id IS NOT NULL
        AND customer_id    IS NOT NULL)     AS expected_rows;

-- ============================================================
-- STEP 9: POST-LOAD CHANNEL NORMALIZATION
-- ============================================================
-- During verification, source_channel values in transactions
-- were found to not match the standardized names in the
-- channels reference table:
--
--   transactions.source_channel  →  channels.channel_name
--   'Organic'                    →  'Organic_search'
--   'Social'                     →  'Social_media'
--
-- Fix applied in two steps:
--   Step 1: Update transactions to use standardized names
--   Step 2: Remove the non-standard entries from channels

-- Step 1: Update transactions to use standard channel names
UPDATE transactions
SET source_channel = 'Organic_search'
WHERE source_channel = 'Organic';

UPDATE transactions
SET source_channel = 'Social_media'
WHERE source_channel = 'Social';

-- Step 2: Remove duplicate/non-standard entries from channels
DELETE FROM channels
WHERE channel_name IN ('organic', 'social');

COMMIT;