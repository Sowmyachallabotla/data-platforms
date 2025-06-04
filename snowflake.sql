-- Create compute warehouse
CREATE OR REPLACE WAREHOUSE db_WH
  WITH WAREHOUSE_SIZE = 'SMALL'
  WAREHOUSE_TYPE = 'STANDARD'
  AUTO_SUSPEND = 300
  AUTO_RESUME = TRUE;

-- Create database and schema
CREATE OR REPLACE DATABASE db_DATA;
USE DATABASE db_DATA;

CREATE OR REPLACE SCHEMA PUBLIC;
USE SCHEMA PUBLIC;

-- Create file format for CSV files
CREATE OR REPLACE FILE FORMAT db_csv_format
  TYPE = 'CSV'
  FIELD_OPTIONALLY_ENCLOSED_BY = '"'
  SKIP_HEADER = 1
  NULL_IF = ('NULL', 'null', '');

-- Create internal stage
CREATE OR REPLACE STAGE db_stage
  FILE_FORMAT = db_csv_format;

-- Create raw staging table (all fields as STRING for initial load)
CREATE OR REPLACE TABLE raw_financial_data (
    transaction_id STRING,
    customer_name STRING,
    account_type STRING,
    transaction_amount STRING,
    currency STRING,
    transaction_date STRING
);

-- Create cleaned table with data types and constraints
CREATE OR REPLACE TABLE clean_financial_data (
    transaction_id STRING PRIMARY KEY,
    customer_name STRING NOT NULL,
    account_type STRING,
    transaction_amount FLOAT,
    currency STRING,
    transaction_date DATE
);
