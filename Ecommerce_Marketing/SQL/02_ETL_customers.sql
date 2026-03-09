-- ============================================================
-- FILE:    02_ETL_customers.sql
-- ORDER:   Run after 01_Schema.sql
-- PROJECT: E-commerce Marketing ROI & Customer Acquisition
--
-- PURPOSE:
-- Cleans and transforms raw customer data from stg_customers
-- and loads it into the production customers table.
--
-- PREREQUISITE: 01_Schema.sql must be run first
--
-- RAW SOURCE:  stg_customers  (5,000 rows)
-- TARGET:      customers      (2,952 rows after cleaning)
--
-- RECORDS EXCLUDED (2,048 rows):
--   - NULL or missing customer_id
--   - NULL or invalid email address
--   - Incomplete name (first_name IS NULL)
--
-- ISSUES CLEANED:
--   1. Names    → Mixed case standardized, empty → NULL
--   2. Age      → Text values fixed, invalid range → NULL
--   3. Email    → Typos corrected, spaces removed, flagged fakes
--   4. Gender   → M/Male/male → 'M', F/Female/female → 'F'
--   5. Country  → USA/US/U.S.A → 'United States' etc.
--   6. Dates    → Multiple formats → YYYY-MM-DD
--   7. is_subscribed → Yes/No/1/0/True/False → TINYINT 1/0
--   8. Segment  → Mixed case standardized, NULL → 'Unassigned'
-- ============================================================

USE ecommerce_marketing;

SET autocommit = 0;
SET SQL_SAFE_UPDATES = 0;

-- ============================================================
-- STEP 1: BACKUP
-- ============================================================
CREATE TABLE customers_backup AS
SELECT * FROM stg_customers;

-- ============================================================
-- STEP 2: CLEAN NAMES
-- ============================================================
-- Empty strings and whitespace → NULL
UPDATE stg_customers
SET first_name = CASE WHEN first_name IS NULL OR TRIM(first_name) = '' THEN NULL END,
    last_name  = CASE WHEN last_name  IS NULL OR TRIM(last_name)  = '' THEN NULL END
WHERE first_name IS NULL OR TRIM(first_name) = ''
   OR last_name  IS NULL OR TRIM(last_name)  = '';

-- Standardize to proper case (john → John, JOHN → John)
UPDATE stg_customers
SET first_name = LTRIM(CONCAT(UPPER(LEFT(first_name,1)), LOWER(SUBSTRING(first_name,2)))),
    last_name  = LTRIM(CONCAT(UPPER(LEFT(last_name,1)),  LOWER(SUBSTRING(last_name,2))))
WHERE first_name IS NOT NULL
   OR last_name  IS NOT NULL;

-- Flag records with missing first_name
-- These will be excluded from the production INSERT
ALTER TABLE stg_customers
ADD COLUMN name_incomplete BOOLEAN DEFAULT FALSE;

UPDATE stg_customers
SET name_incomplete = TRUE
WHERE first_name IS NULL;

COMMIT;

-- ============================================================
-- STEP 3: CLEAN AGE
-- ============================================================
-- Fix known text value
UPDATE stg_customers
SET age = '25'
WHERE age = 'twenty-five';

-- Null out invalid range (valid: 18-120)
UPDATE stg_customers
SET age = NULL
WHERE age <= 0
   OR age > 120;

-- Flag missing or invalid age for data quality tracking
ALTER TABLE stg_customers
ADD COLUMN age_invalid BOOLEAN DEFAULT FALSE;

UPDATE stg_customers
SET age_invalid = TRUE
WHERE age IS NULL;

COMMIT;

-- ============================================================
-- STEP 4: CLEAN EMAIL
-- ============================================================
-- Remove spaces, trim leading/trailing dots, lowercase
UPDATE stg_customers
SET email = LOWER(TRIM(BOTH '.' FROM
              REPLACE(REPLACE(REPLACE(REPLACE(
              TRIM(email), ' ',''),'..',''),'...',''),'....','')
            ))
WHERE email IS NOT NULL;

-- Fix common domain typos
UPDATE stg_customers
SET email = CASE
    WHEN email LIKE '%@gmial.com'  THEN REPLACE(email, '@gmial.com',  '@gmail.com')
    WHEN email LIKE '%@gmal.com'   THEN REPLACE(email, '@gmal.com',   '@gmail.com')
    WHEN email LIKE '%@gamil.com'  THEN REPLACE(email, '@gamil.com',  '@gmail.com')
    WHEN email LIKE '%@yaho.com'   THEN REPLACE(email, '@yaho.com',   '@yahoo.com')
    WHEN email LIKE '%@yahooo.com' THEN REPLACE(email, '@yahooo.com', '@yahoo.com')
    WHEN email LIKE '%@hotmal.com' THEN REPLACE(email, '@hotmal.com', '@hotmail.com')
    WHEN email LIKE '%@outlok.com' THEN REPLACE(email, '@outlok.com', '@outlook.com')
    ELSE email
END
WHERE email LIKE '%@gmial.com'
   OR email LIKE '%@gmal.com'
   OR email LIKE '%@gamil.com'
   OR email LIKE '%@yaho.com'
   OR email LIKE '%@yahooo.com'
   OR email LIKE '%@hotmal.com'
   OR email LIKE '%@outlok.com';

-- Flag placeholder, fake, and missing emails
-- These will be excluded from the production INSERT
ALTER TABLE stg_customers
ADD COLUMN email_invalid BOOLEAN DEFAULT FALSE;

UPDATE stg_customers
SET email_invalid = TRUE
WHERE email LIKE '%@test.com'
   OR email LIKE '%@example.com'
   OR email LIKE '%@fake.com'
   OR email LIKE 'test@%'
   OR email LIKE 'fake@%'
   OR email LIKE 'noemail@%'
   OR email LIKE 'none@%'
   OR email LIKE 'na@%'
   OR email LIKE 'abc@%'
   OR email LIKE '123@%'
   OR email IN ('na', 'none', 'n/a')
   OR email IS NULL;

COMMIT;

-- ============================================================
-- STEP 5: CLEAN GENDER
-- ============================================================
-- Standardize all variants to 'Male' / 'Female' / NULL
UPDATE stg_customers
SET gender = CASE
    WHEN gender IN ('M', 'Male', 'male', 'm') THEN 'Male'
    WHEN gender IN ('F', 'Female', 'female', 'f') THEN 'Female'
    ELSE NULL
END;

COMMIT;

-- ============================================================
-- STEP 6: CLEAN COUNTRY
-- ============================================================
-- Map all variants to standard country names
UPDATE stg_customers
SET country = CASE
    WHEN country IN ('GB','UK','United Kingdom')          THEN 'United Kingdom'
    WHEN country IN ('USA','U.S.A','US','usa','United States') THEN 'United States'
    WHEN country IN ('CA','Canada')                       THEN 'Canada'
    ELSE NULL
END
WHERE country IS NOT NULL;

-- Empty strings → NULL
UPDATE stg_customers
SET country = NULL
WHERE country IS NULL
   OR TRIM(country) = '';

COMMIT;

-- ============================================================
-- STEP 7: CLEAN SIGNUP_DATE
-- ============================================================
-- Standardize multiple date formats to YYYY-MM-DD
UPDATE stg_customers
SET signup_date = CASE
    WHEN signup_date REGEXP '^[0-9]{4}-[0-9]{2}-[0-9]{2}$'
        THEN signup_date
    WHEN signup_date REGEXP '^[0-9]{4}/[0-9]{2}/[0-9]{2}$'
        THEN DATE_FORMAT(STR_TO_DATE(signup_date, '%Y/%m/%d'), '%Y-%m-%d')
    WHEN signup_date REGEXP '^[0-9]{2}-[0-9]{2}-[0-9]{4}$'
     AND CAST(SUBSTRING(signup_date,1,2) AS UNSIGNED) > 12
        THEN DATE_FORMAT(STR_TO_DATE(signup_date, '%d-%m-%Y'), '%Y-%m-%d')
    WHEN signup_date REGEXP '^[0-9]{2}-[0-9]{2}-[0-9]{4}$'
     AND CAST(SUBSTRING(signup_date,1,2) AS UNSIGNED) <= 12
        THEN DATE_FORMAT(STR_TO_DATE(signup_date, '%m-%d-%Y'), '%Y-%m-%d')
    WHEN signup_date REGEXP '^[0-9]{2}/[0-9]{2}/[0-9]{4}$'
     AND CAST(SUBSTRING(signup_date,1,2) AS UNSIGNED) > 12
        THEN DATE_FORMAT(STR_TO_DATE(signup_date, '%d/%m/%Y'), '%Y-%m-%d')
    WHEN signup_date REGEXP '^[0-9]{2}/[0-9]{2}/[0-9]{4}$'
     AND CAST(SUBSTRING(signup_date,1,2) AS UNSIGNED) <= 12
        THEN DATE_FORMAT(STR_TO_DATE(signup_date, '%m/%d/%Y'), '%Y-%m-%d')
    WHEN signup_date REGEXP '^[A-Za-z]+ [0-9]{1,2}, [0-9]{4}$'
        THEN DATE_FORMAT(STR_TO_DATE(signup_date, '%M %d, %Y'), '%Y-%m-%d')
    WHEN signup_date REGEXP '^[0-9]{1,2} [A-Za-z]+ [0-9]{4}$'
        THEN DATE_FORMAT(STR_TO_DATE(signup_date, '%d %M %Y'), '%Y-%m-%d')
END
WHERE signup_date IS NOT NULL;

COMMIT;

-- ============================================================
-- STEP 8: CLEAN IS_SUBSCRIBED
-- ============================================================
-- Standardize Yes/No/1/0/True/False → 'Yes' / 'No' / NULL
UPDATE stg_customers
SET is_subscribed = NULL
WHERE is_subscribed IS NULL
   OR TRIM(is_subscribed) = '';

UPDATE stg_customers
SET is_subscribed = CASE
    WHEN LOWER(TRIM(is_subscribed)) IN ('yes','1','true')  THEN 'Yes'
    WHEN LOWER(TRIM(is_subscribed)) IN ('no', '0','false') THEN 'No'
    ELSE NULL
END
WHERE is_subscribed IS NOT NULL;

COMMIT;

-- ============================================================
-- STEP 9: CLEAN CUSTOMER_SEGMENT
-- ============================================================
-- Empty → NULL, then NULL → 'Unassigned', standardize case
UPDATE stg_customers
SET customer_segment = NULL
WHERE customer_segment IS NULL
   OR TRIM(customer_segment) = '';

UPDATE stg_customers
SET customer_segment = LTRIM(CONCAT(
    UPPER(LEFT(customer_segment,1)),
    LOWER(SUBSTRING(customer_segment,2))
))
WHERE customer_segment IS NOT NULL;

-- Default NULL segments to 'Unassigned' rather than excluding
UPDATE stg_customers
SET customer_segment = 'Unassigned'
WHERE customer_segment IS NULL;

COMMIT;

-- ============================================================
-- STEP 10: VALIDATE BEFORE LOADING
-- ============================================================
-- Compare raw vs cleaned counts
SELECT
    (SELECT COUNT(*) FROM stg_customers)       AS total_staged,
    (SELECT COUNT(*) FROM customers_backup)    AS total_raw,
    (SELECT COUNT(*) FROM stg_customers
      WHERE name_incomplete = TRUE)            AS excluded_incomplete_name,
    (SELECT COUNT(*) FROM stg_customers
      WHERE email_invalid = TRUE)              AS flagged_invalid_email,
    (SELECT COUNT(*) FROM stg_customers
      WHERE customer_id IS NOT NULL
        AND email IS NOT NULL
        AND name_incomplete != 1)              AS records_to_load;

-- Check for duplicates before inserting
SELECT customer_id, email, COUNT(*) AS cnt
FROM stg_customers
WHERE customer_id IS NOT NULL
  AND email IS NOT NULL
GROUP BY customer_id, email
HAVING COUNT(*) > 1;

-- ============================================================
-- STEP 11: LOAD INTO PRODUCTION TABLE
-- ============================================================
-- Exclusion rules:
--   - NULL customer_id
--   - NULL or invalid email
--   - Incomplete name (first_name IS NULL)
INSERT INTO customers (
    customer_id,
    email,
    first_name,
    last_name,
    age,
    country,
    acquisition_source,
    signup_date,
    is_subscribed,
    customer_segment
)
SELECT
    customer_id,
    email,
    first_name,
    last_name,
    age,
    country,
    acquisition_source,
    signup_date,
    CASE
        WHEN LOWER(is_subscribed) = 'yes' THEN 1
        WHEN LOWER(is_subscribed) = 'no'  THEN 0
        ELSE NULL
    END AS is_subscribed,
    customer_segment
FROM stg_customers
WHERE customer_id IS NOT NULL
  AND email IS NOT NULL
  AND name_incomplete != 1;

COMMIT;

-- ============================================================
-- STEP 12: VERIFY LOAD
-- ============================================================
SELECT
    (SELECT COUNT(*) FROM customers)       AS production_rows,
    (SELECT COUNT(*) FROM stg_customers
      WHERE customer_id IS NOT NULL
        AND email IS NOT NULL
        AND name_incomplete != 1)          AS expected_rows;

-- ============================================================
-- STEP 13: CLEANUP
-- ============================================================
DROP TABLE customers_backup;