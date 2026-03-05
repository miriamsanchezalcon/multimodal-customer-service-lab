-- ============================================================================
-- MULTIMODAL CUSTOMER SERVICE LAB - SETUP SCRIPT
-- ============================================================================
-- This script creates all required database objects and loads sample data
-- Run this BEFORE opening the notebook
-- Estimated runtime: 2-3 minutes
-- ============================================================================

-- ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
-- STEP 1: CREATE YOUR WORKSPACE
-- ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
-- A DATABASE is a top-level container (like a folder) for all your objects.
-- A SCHEMA is a namespace inside it (like a subfolder).
-- Everything we build today lives in MULTIMODAL_CUSTOMER_SERVICE.DATA.
CREATE DATABASE IF NOT EXISTS MULTIMODAL_CUSTOMER_SERVICE;
USE DATABASE MULTIMODAL_CUSTOMER_SERVICE;
CREATE SCHEMA IF NOT EXISTS DATA;
USE SCHEMA MULTIMODAL_CUSTOMER_SERVICE.DATA;

-- ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
-- STEP 2: CREATE COMPUTE
-- ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
-- A WAREHOUSE is your compute engine — it runs queries and AI functions.
-- In Snowflake, compute is separate from storage, so you can scale it
-- up or down without touching your data.
-- MEDIUM gives us enough power for audio transcription.
-- AUTO_SUSPEND = 300 means it shuts off after 5 minutes of inactivity
-- so you're not paying for idle compute.
CREATE WAREHOUSE IF NOT EXISTS CALL_CENTER_WH
    WAREHOUSE_SIZE = 'MEDIUM'
    AUTO_SUSPEND = 300
    AUTO_RESUME = TRUE
    INITIALLY_SUSPENDED = TRUE;

USE WAREHOUSE CALL_CENTER_WH;

-- ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
-- STEP 3: ENABLE CORTEX AI
-- ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
-- This enables Snowflake's built-in AI functions (transcription, translation,
-- sentiment analysis, etc.) regardless of which cloud region your account is in.
-- No API keys, no external services — everything runs inside Snowflake.
ALTER ACCOUNT SET CORTEX_ENABLED_CROSS_REGION = 'ANY_REGION';

-- ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
-- STEP 4: LOAD AUDIO FILES
-- ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
-- A STAGE is a file storage area — think of it as a loading dock.
-- An EXTERNAL stage points to files in cloud storage (S3 in this case).
-- An INTERNAL stage holds files copied into Snowflake for processing.
-- We copy from external → internal so Cortex AI can access the files directly.

-- External stage: points to sample audio files in S3
CREATE OR REPLACE STAGE MULTIMODAL_CUSTOMER_SERVICE.DATA.CUSTOMER_CALLS_EXTERNAL
  URL = 's3://sfquickstarts/extracting-insights-from-multimodal-customer-data/AUDIO_DATA/'
  DIRECTORY = (ENABLE = TRUE);

-- Internal stage: where we'll store the files inside Snowflake
CREATE OR REPLACE STAGE MULTIMODAL_CUSTOMER_SERVICE.DATA.CUSTOMER_CALLS
    ENCRYPTION = (TYPE = 'SNOWFLAKE_SSE')
    DIRECTORY = (ENABLE = TRUE);

-- Copy all 101 audio files from S3 into our internal stage
COPY FILES
  INTO @CUSTOMER_CALLS
  FROM @CUSTOMER_CALLS_EXTERNAL;

-- Refresh the directory so Snowflake knows what files are available
ALTER STAGE MULTIMODAL_CUSTOMER_SERVICE.DATA.CUSTOMER_CALLS REFRESH;

-- Create a table listing just 5 audio files (out of 101) to keep lab timing manageable
CREATE OR REPLACE TABLE DATA.audio_file_list AS 
SELECT 
    RELATIVE_PATH AS file_name
FROM DIRECTORY(@MULTIMODAL_CUSTOMER_SERVICE.DATA.Customer_Calls)
LIMIT 5;

-- ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
-- STEP 5: LOAD PDF DOCUMENTS
-- ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
-- Same pattern: external stage → internal stage → ready for AI processing

-- External stage: points to sample PDF documents in S3
CREATE OR REPLACE STAGE MULTIMODAL_CUSTOMER_SERVICE.DATA.COMPANY_DOCUMENTS_EXTERNAL
  URL = 's3://sfquickstarts/extracting-insights-from-multimodal-customer-data/DOCUMENT_DATA/'
  DIRECTORY = (ENABLE = TRUE);

-- Internal stage for documents
CREATE OR REPLACE STAGE MULTIMODAL_CUSTOMER_SERVICE.DATA.COMPANY_DOCUMENTS
    ENCRYPTION = (TYPE = 'SNOWFLAKE_SSE')
    DIRECTORY = (ENABLE = TRUE);

-- Copy ~70 PDF documents from S3
COPY FILES
  INTO @COMPANY_DOCUMENTS
  FROM @COMPANY_DOCUMENTS_EXTERNAL;

-- Refresh directory listing
ALTER STAGE COMPANY_DOCUMENTS REFRESH;

-- ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
-- STEP 6: CREATE RESULTS TABLE
-- ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
-- This empty table will hold the output of our AI processing pipeline.
-- The VARIANT columns (segments, raw_response) store JSON data — Snowflake
-- handles semi-structured data natively, no parsing needed.
-- The notebook will populate this table in Module 1.
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

-- ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
-- STEP 7: LOAD STRUCTURED DATA (CHAT LOGS & SUPPORT TICKETS)
-- ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
-- Now we load traditional tabular data from CSV files.
-- INFER_SCHEMA automatically detects column names and types from the CSV header.
-- USING TEMPLATE creates the table with those inferred columns — no need to
-- manually define the schema. COPY INTO then loads the actual data.

-- Stage pointing to CSV files in S3
CREATE OR REPLACE STAGE MULTIMODAL_CUSTOMER_SERVICE.DATA.TABLE_DATA
  URL = 's3://sfquickstarts/extracting-insights-from-multimodal-customer-data/TABLE_DATA/'
  DIRECTORY = (ENABLE = TRUE);

-- File format tells Snowflake how to parse the CSV
CREATE OR REPLACE FILE FORMAT csv_format
  TYPE = 'CSV'
  PARSE_HEADER = TRUE
  FIELD_OPTIONALLY_ENCLOSED_BY = '"'
  TRIM_SPACE = TRUE
  EMPTY_FIELD_AS_NULL = TRUE;

-- Chat logs: 100 customer chat transcripts with agent-assigned categories
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

-- Support tickets: 100 formal tickets linked to the chats above
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

-- ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
-- STEP 8: VERIFY EVERYTHING WORKED
-- ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
-- You should see: audio_file_count = 5, chat_log_count = 100, ticket_count = 100
-- If any count is 0, re-run the corresponding section above.

-- Check audio files loaded
SELECT COUNT(*) AS audio_file_count FROM DATA.audio_file_list;

-- Check chat logs loaded
SELECT COUNT(*) AS chat_log_count FROM CHAT_LOGS;

-- Check support tickets loaded
SELECT COUNT(*) AS ticket_count FROM SUPPORT_TICKETS;

-- List stages
SHOW STAGES IN SCHEMA DATA;

SELECT '✅ Setup complete! You can now open the notebook.' AS status;
