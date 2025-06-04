"# Data Platform" 


This repository contains a  data engineering pipeline for  data platform, showcasing ingestion, transformation, and orchestration using AWS, Snowflake, Airflow, and dbt.

---

## Project Overview

This project simulates a daily ETL pipeline that:

- Pulls  financial data (CSV) and uploads it to an S3 bucket
- Loads the raw data into Snowflake staging tables
- Transforms the raw data into a clean, type-safe format using dbt models
- Orchestrates the entire pipeline using Apache Airflow DAGs with Python and Snowflake operators

---

## Technologies Used

- **AWS S3**: Cloud storage for raw data files
- **Snowflake**: Data warehouse for staging and transformed tables
- **Apache Airflow**: Workflow orchestration and scheduling
- **dbt**: Data transformation and modeling
- **Python**: Data ingestion script and Airflow operators

---

## Setup Instructions

### Prerequisites

- Python 3.7+
- AWS CLI configured with appropriate credentials
- Snowflake account and configured Airflow Snowflake connection
- Apache Airflow installed and configured
- dbt installed and configured for Snowflake

### Steps

1. **Clone this repository**

```bash
git clone https://github.com/Sowmyachallabotla/data-platforms.git
cd data-platform

Upload mock data to S3

Run the ingestion script to upload the mock financial CSV data to your S3 bucket:


python scripts/ingest_data.py
Create Snowflake schema and tables

Run the SQL script (snowflake_schema.sql) in your Snowflake environment to set up tables and file formats.

Run Airflow DAG

Add the daily_etl_pipeline.py DAG file to your Airflow DAGs folder.

Start Airflow scheduler and webserver.

Trigger the DAG manually or wait for scheduled runs.

Run dbt transformation

From your dbt project directory, run:


dbt run --models transformation
Verify results

Query your Snowflake clean_financial_data table to verify clean transformed data.

Project Structure


Edit
├── scripts/
│   └── ingest_data.py              # Uploads mock data CSV to S3
├── dags/
│   └── daily_etl_pipeline.py       # Airflow DAG for ETL orchestration
├── models/
│   └── transformation.sql          # dbt model for data transformation
├── snowflake_schema.sql            # Snowflake schema and file format setup
├── mock_financial_data.csv         # Sample raw financial data CSV
└── README.md                      # This file

Contact
For questions or feedback, reach out to challasowmya29@gmail.com.








---

### End-to-End Testing Checklist

1. **Prepare environment**
   - AWS CLI configured and S3 bucket ready
   - Snowflake account and schema ready
   - Airflow installed, running, with Snowflake connection configured
   - dbt configured and connected to Snowflake

2. **Test ingestion**
   - Run `python scripts/ingest_data.py`
   - Verify `mock_financial_data.csv` uploaded to S3 under specified bucket/key

3. **Test Snowflake schema**
   - Run `snowflake_schema.sql`
   - Verify tables `raw_financial_data` and `clean_financial_data` exist

4. **Test Airflow DAG**
   - Place `daily_etl_pipeline.py` in Airflow DAGs folder
   - Trigger DAG manually
   - Verify tasks run in order: ingestion script → copy to Snowflake → transform data

5. **Test dbt model**
   - Run `dbt run --models transformation`
   - Verify transformed data is clean and properly typed in `clean_financial_data`

6. **Validate data quality**
   - Run queries on `clean_financial_data`
   - Check for no NULLs in key columns like `transaction_amount` and `transaction_date`
   - Check for correct data types

---





