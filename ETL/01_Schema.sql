-- =====================================================
-- FILE:    01_Schema.sql
-- PROJECT: E-commerce Marketing ROI & Customer Acquisition
--
-- PURPOSE:
-- Creates the database, staging tables (for raw CSV import),
-- reference tables, and production tables (with constraints).
-- =====================================================
-- DATABASE CREATION
-- =====================================================
CREATE DATABASE IF NOT EXISTS ecommerce_marketing;
USE ecommerce_marketing;

-- #####################################################
-- PART 1: STAGING TABLES (For Raw Data Import)
-- No Foreign Keys - Accept messy data as-is
-- #####################################################

-- =====================================================
-- STAGING TABLE 1: STG_CUSTOMERS
-- =====================================================
DROP TABLE IF EXISTS stg_customers;
CREATE TABLE stg_customers (
    customer_id VARCHAR(50),
    email VARCHAR(255),
    first_name VARCHAR(100),
    last_name VARCHAR(100),
    age VARCHAR(50),
    gender VARCHAR(50),
    country VARCHAR(100),
    acquisition_source VARCHAR(100),
    signup_date VARCHAR(50),
    is_subscribed VARCHAR(50),
    customer_segment VARCHAR(50)
);

-- =====================================================
-- STAGING TABLE 2: STG_EMAIL_CAMPAIGNS
-- =====================================================
DROP TABLE IF EXISTS stg_email_campaigns;
CREATE TABLE stg_email_campaigns (
    campaign_id VARCHAR(50),
    campaign_name VARCHAR(255),
    variant VARCHAR(10),
    subject_line VARCHAR(500),
    send_date VARCHAR(50),
    send_time VARCHAR(50),
    campaign_type VARCHAR(100),
    discount_offered VARCHAR(50),
    total_sent VARCHAR(50),
    marketing_cost VARCHAR(50)
);

-- =====================================================
-- STAGING TABLE 3: STG_EMAIL_EVENTS
-- =====================================================
DROP TABLE IF EXISTS stg_email_events;
CREATE TABLE stg_email_events (
    event_id VARCHAR(50),
    campaign_id VARCHAR(50),
    customer_id VARCHAR(50),
    email VARCHAR(255),
    sent_status VARCHAR(50),
    opened VARCHAR(50),
    open_timestamp VARCHAR(50),
    clicked VARCHAR(50),
    click_timestamp VARCHAR(50),
    unsubscribed VARCHAR(50),
    device_type VARCHAR(50),
    email_client VARCHAR(100)
);

-- =====================================================
-- STAGING TABLE 4: STG_TRANSACTIONS
-- =====================================================
DROP TABLE IF EXISTS stg_transactions;
CREATE TABLE stg_transactions (
    transaction_id VARCHAR(50),
    customer_id VARCHAR(50),
    transaction_date VARCHAR(50),
    revenue VARCHAR(50),
    quantity VARCHAR(50),
    product_category VARCHAR(100),
    discount_applied VARCHAR(50),
    payment_method VARCHAR(100),
    order_status VARCHAR(50),
    source_channel VARCHAR(100),
    campaign_id VARCHAR(50)
);

-- =====================================================
-- STAGING TABLE 5: STG_MARKETING_SPEND
-- =====================================================
DROP TABLE IF EXISTS stg_marketing_spend;
CREATE TABLE stg_marketing_spend (
    date VARCHAR(50),
    channel VARCHAR(100),
    spend_amount VARCHAR(50),
    impressions VARCHAR(50),
    clicks VARCHAR(50),
    conversions VARCHAR(50),
    currency VARCHAR(50)
);

-- =====================================================
-- STAGING TABLE 6: STG_WEBSITE_SESSIONS
-- =====================================================
DROP TABLE IF EXISTS stg_website_sessions;
CREATE TABLE stg_website_sessions (
    session_id VARCHAR(50),
    customer_id VARCHAR(50),
    session_date VARCHAR(50),
    session_start_time VARCHAR(50),
    traffic_source VARCHAR(100),
    landing_page VARCHAR(255),
    pages_viewed VARCHAR(50),
    session_duration_seconds VARCHAR(50),
    device VARCHAR(50),
    browser VARCHAR(100),
    converted VARCHAR(50),
    conversion_value VARCHAR(50),
    campaign_id VARCHAR(50),
    is_bounce VARCHAR(50),
    is_bot VARCHAR(50)
);

-- #####################################################
-- PART 2: FINAL CLEAN TABLES (With Constraints)
-- Create these AFTER cleaning the staging data
-- #####################################################

-- =====================================================
-- REFERENCE TABLE: CHANNELS (New - for normalization)
-- =====================================================
DROP TABLE IF EXISTS channels;
CREATE TABLE channels (
    channel_id INT AUTO_INCREMENT PRIMARY KEY,
    channel_name VARCHAR(50) NOT NULL UNIQUE,
    channel_type VARCHAR(50)
);

-- Insert standard channel values
INSERT INTO channels (channel_name, channel_type) VALUES
    ('email', 'owned'),
    ('paid_search', 'paid'),
    ('organic_search', 'organic'),
    ('social_media', 'paid'),
    ('display_ads', 'paid'),
    ('affiliate', 'paid'),
    ('referral', 'organic'),
    ('direct', 'organic'),
    ('social', 'paid'),
    ('organic', 'organic');


-- =====================================================
-- FINAL TABLE 1: CUSTOMERS (Master Table)
-- =====================================================
DROP TABLE IF EXISTS customers;
CREATE TABLE customers (
    customer_id INT PRIMARY KEY,
    email VARCHAR(255) NOT NULL UNIQUE,
    first_name VARCHAR(100),
    last_name VARCHAR(100),
    age INT,
    gender CHAR(1),
    country VARCHAR(100),
    acquisition_source VARCHAR(50),
    signup_date DATE,
    is_subscribed TINYINT,
    customer_segment VARCHAR(50),
    
    INDEX idx_acquisition_source (acquisition_source),
    INDEX idx_signup_date (signup_date)
);

-- =====================================================
-- FINAL TABLE 2: EMAIL_CAMPAIGNS (Master Table)
-- =====================================================
DROP TABLE IF EXISTS email_campaigns;
CREATE TABLE email_campaigns (
    campaign_id INT PRIMARY KEY,
    campaign_name VARCHAR(255),
    variant CHAR(1),
    subject_line VARCHAR(500),
    send_date DATE,
    send_time TIME,
    campaign_type VARCHAR(50),
    discount_percent INT,
    total_sent INT,
    marketing_cost DECIMAL(10,2),
    
    INDEX idx_variant (variant),
    INDEX idx_send_date (send_date)
);


-- =====================================================
-- FINAL TABLE 3: EMAIL_EVENTS (Transaction Table)
-- =====================================================
DROP TABLE IF EXISTS email_events;
CREATE TABLE email_events (
    event_id INT PRIMARY KEY,
    campaign_id INT NOT NULL,
    customer_id INT NOT NULL,
    email VARCHAR(255),
    sent_status VARCHAR(50),
    opened TINYINT,
    open_timestamp DATETIME,
    clicked TINYINT,
    click_timestamp DATETIME,
    unsubscribed TINYINT,
    device_type VARCHAR(50),
    email_client VARCHAR(100),
    
    FOREIGN KEY (campaign_id) REFERENCES email_campaigns(campaign_id),
    FOREIGN KEY (customer_id) REFERENCES customers(customer_id),
    
    UNIQUE KEY unique_campaign_customer (campaign_id, customer_id),
    
    INDEX idx_campaign (campaign_id),
    INDEX idx_customer (customer_id),
    INDEX idx_opened (opened),
    INDEX idx_clicked (clicked)
);


-- =====================================================
-- FINAL TABLE 4: TRANSACTIONS (Transaction Table)
-- =====================================================
DROP TABLE IF EXISTS transactions;
CREATE TABLE transactions (
    transaction_id INT PRIMARY KEY,
    customer_id INT NOT NULL,
    transaction_date DATE,
    revenue DECIMAL(10,2),
    quantity INT,
    product_category VARCHAR(100),
    discount_applied DECIMAL(5,2),
    payment_method VARCHAR(100),
    order_status VARCHAR(50),
    source_channel VARCHAR(50),
    campaign_id INT,
    
    FOREIGN KEY (customer_id) REFERENCES customers(customer_id),
    FOREIGN KEY (campaign_id) REFERENCES email_campaigns(campaign_id),
    
    INDEX idx_customer (customer_id),
    INDEX idx_transaction_date (transaction_date),
    INDEX idx_source_channel (source_channel),
    INDEX idx_campaign (campaign_id)
);


-- =====================================================
-- FINAL TABLE 5: MARKETING_SPEND (Fact Table)
-- =====================================================
DROP TABLE IF EXISTS marketing_spend;
CREATE TABLE marketing_spend (
    spend_id INT AUTO_INCREMENT PRIMARY KEY,
    spend_date DATE NOT NULL,
    channel VARCHAR(100) NOT NULL,
    spend_amount DECIMAL(10,2),
    impressions INT,
    clicks INT,
    conversions INT,
    
    UNIQUE KEY unique_date_channel (spend_date, channel),
    
    INDEX idx_channel (channel),
    INDEX idx_date (spend_date)
);


-- =====================================================
-- FINAL TABLE 6: WEBSITE_SESSIONS (Transaction Table)
-- =====================================================
DROP TABLE IF EXISTS website_sessions;
CREATE TABLE website_sessions (
    session_id INT PRIMARY KEY,
    customer_id INT,
    session_date DATE,
    session_start_time TIME,
    traffic_source VARCHAR(100),
    landing_page VARCHAR(255),
    pages_viewed INT,
    session_duration_seconds INT,
    device VARCHAR(50),
    browser VARCHAR(100),
    converted TINYINT,
    conversion_value DECIMAL(10,2),
    campaign_id INT,
    is_bounce TINYINT,
    
    FOREIGN KEY (customer_id) REFERENCES customers(customer_id),
    FOREIGN KEY (campaign_id) REFERENCES email_campaigns(campaign_id),
    
    INDEX idx_customer (customer_id),
    INDEX idx_session_date (session_date),
    INDEX idx_traffic_source (traffic_source),
    INDEX idx_converted (converted),
    INDEX idx_campaign (campaign_id)
);

-- #####################################################
-- INSERT ORDER FOR TABLES:
-- 1. customers
-- 2. email_campaigns
-- 3. marketing_spend
-- 4. email_events
-- 5. transactions
-- 6. website_sessions
-- 7. channels
-- 
-- (Order doesn't matter for staging since no FKs)
-- #####################################################
