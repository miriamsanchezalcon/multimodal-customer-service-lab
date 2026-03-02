-- ============================================================================
-- MULTIMODAL CUSTOMER SERVICE LAB - SETUP SCRIPT
-- ============================================================================
-- This script creates all required database objects and loads sample data
-- Run this BEFORE opening the notebook
-- Estimated runtime: 2-3 minutes
-- ============================================================================

-- Create database and schema
CREATE DATABASE IF NOT EXISTS MULTIMODAL_CUSTOMER_SERVICE;
USE DATABASE MULTIMODAL_CUSTOMER_SERVICE;
CREATE SCHEMA IF NOT EXISTS DATA;
USE SCHEMA MULTIMODAL_CUSTOMER_SERVICE.DATA;

-- Enable cross-region Cortex AI (required for some regions)
ALTER ACCOUNT SET CORTEX_ENABLED_CROSS_REGION = 'ANY_REGION';

-- ============================================================================
-- AUDIO FILES SETUP
-- ============================================================================
-- Create external stage pointing to sample audio files
CREATE OR REPLACE STAGE MULTIMODAL_CUSTOMER_SERVICE.DATA.CUSTOMER_CALLS_EXTERNAL
  URL = 's3://sfquickstarts/extracting-insights-from-multimodal-customer-data/AUDIO_DATA/'
  DIRECTORY = (ENABLE = TRUE);

-- Create internal stage for audio files
CREATE OR REPLACE STAGE MULTIMODAL_CUSTOMER_SERVICE.DATA.CUSTOMER_CALLS
    ENCRYPTION = (TYPE = 'SNOWFLAKE_SSE')
    DIRECTORY = (ENABLE = TRUE);

-- Copy audio files to internal stage
COPY FILES
  INTO @CUSTOMER_CALLS
  FROM @CUSTOMER_CALLS_EXTERNAL;

-- Refresh stage directory
ALTER STAGE MULTIMODAL_CUSTOMER_SERVICE.DATA.CUSTOMER_CALLS REFRESH;

-- Create table listing audio files (limit to 5 for lab timing — stage has 101 files)
CREATE OR REPLACE TABLE DATA.audio_file_list AS 
SELECT 
    RELATIVE_PATH AS file_name
FROM DIRECTORY(@MULTIMODAL_CUSTOMER_SERVICE.DATA.Customer_Calls)
LIMIT 5;

-- ============================================================================
-- PDF DOCUMENTS SETUP
-- ============================================================================
-- Create external stage pointing to sample PDF documents
CREATE OR REPLACE STAGE MULTIMODAL_CUSTOMER_SERVICE.DATA.COMPANY_DOCUMENTS_EXTERNAL
  URL = 's3://sfquickstarts/extracting-insights-from-multimodal-customer-data/DOCUMENT_DATA/'
  DIRECTORY = (ENABLE = TRUE);

-- Create internal stage for documents
CREATE OR REPLACE STAGE MULTIMODAL_CUSTOMER_SERVICE.DATA.COMPANY_DOCUMENTS
    ENCRYPTION = (TYPE = 'SNOWFLAKE_SSE')
    DIRECTORY = (ENABLE = TRUE);

-- Copy documents to internal stage
COPY FILES
  INTO @COMPANY_DOCUMENTS
  FROM @COMPANY_DOCUMENTS_EXTERNAL;

-- Refresh stage directory
ALTER STAGE COMPANY_DOCUMENTS REFRESH;

-- ============================================================================
-- RESULTS TABLE
-- ============================================================================
-- Create table to store transcription and analysis results
CREATE TABLE IF NOT EXISTS transcription_results (
    transcription_id VARCHAR(100) PRIMARY KEY DEFAULT ('trans_' || UUID_STRING()),
    stage_location VARCHAR(500) NOT NULL,
    file_name VARCHAR(255) NOT NULL,
    timestamp_granularity VARCHAR(20) DEFAULT 'speaker',
    audio_duration FLOAT NOT NULL,
    segments VARIANT NOT NULL,
    raw_response VARIANT NOT NULL,
    translated_text VARCHAR,
    call_category VARCHAR,
    sentiment_label VARCHAR(20),
    call_summary VARCHAR,
    created_at TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),
    transcription_completed_at TIMESTAMP_NTZ
);

-- ============================================================================
-- CHAT LOGS & SUPPORT TICKETS SETUP
-- ============================================================================
-- Create stage for CSV data
CREATE OR REPLACE STAGE MULTIMODAL_CUSTOMER_SERVICE.DATA.TABLE_DATA
  URL = 's3://sfquickstarts/extracting-insights-from-multimodal-customer-data/TABLE_DATA/'
  DIRECTORY = (ENABLE = TRUE);

-- Create file format for CSV parsing
CREATE OR REPLACE FILE FORMAT csv_format
  TYPE = 'CSV'
  PARSE_HEADER = TRUE
  FIELD_OPTIONALLY_ENCLOSED_BY = '"'
  TRIM_SPACE = TRUE
  EMPTY_FIELD_AS_NULL = TRUE;

-- Create and load CHAT_LOGS table
CREATE OR REPLACE TABLE CHAT_LOGS
USING TEMPLATE (
  SELECT ARRAY_AGG(OBJECT_CONSTRUCT(*))
  FROM TABLE(
    INFER_SCHEMA(
      LOCATION => '@TABLE_DATA/chat_logs.csv',
      FILE_FORMAT => 'csv_format'
    )
  )
);

COPY INTO CHAT_LOGS
FROM @TABLE_DATA/chat_logs.csv
FILE_FORMAT = (FORMAT_NAME = 'csv_format')
MATCH_BY_COLUMN_NAME = CASE_INSENSITIVE;

-- Create and load SUPPORT_TICKETS table
CREATE OR REPLACE TABLE SUPPORT_TICKETS
USING TEMPLATE (
  SELECT ARRAY_AGG(OBJECT_CONSTRUCT(*))
  FROM TABLE(
    INFER_SCHEMA(
      LOCATION => '@TABLE_DATA/support_tickets.csv',
      FILE_FORMAT => 'csv_format'
    )
  )
);

COPY INTO SUPPORT_TICKETS
FROM @TABLE_DATA/support_tickets.csv
FILE_FORMAT = (FORMAT_NAME = 'csv_format')
MATCH_BY_COLUMN_NAME = CASE_INSENSITIVE;

-- ============================================================================
-- VERIFICATION
-- ============================================================================
-- Run these queries to verify setup completed successfully

-- Check audio files loaded
SELECT COUNT(*) AS audio_file_count FROM DATA.audio_file_list;

-- Check chat logs loaded
SELECT COUNT(*) AS chat_log_count FROM CHAT_LOGS;

-- Check support tickets loaded
SELECT COUNT(*) AS ticket_count FROM SUPPORT_TICKETS;

-- List stages
SHOW STAGES IN SCHEMA DATA;

SELECT '✅ Setup complete! You can now open the notebook.' AS status;
