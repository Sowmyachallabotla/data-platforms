import snowflake.connector
import logging
from datetime import datetime

# Configure logging
logging.basicConfig(
    filename='daily_etl_pipeline.log',
    level=logging.INFO,
    format='%(asctime)s %(levelname)s: %(message)s'
)

# Snowflake connection parameters (replace placeholders)
conn_params = {
    'user': '<your_username>',
    'password': '<your_password>',
    'account': '<your_account_identifier>',
    'warehouse': 'data_WH',
    'database': 'drt_DATA',
    'schema': 'PUBLIC'
}

def connect_snowflake():
    """Create and return a Snowflake connection."""
    try:
        conn = snowflake.connector.connect(**conn_params)
        logging.info("Connected to Snowflake successfully.")
        return conn
    except Exception as e:
        logging.error(f"Failed to connect to Snowflake: {e}")
        raise

def execute_query(conn, query):
    """Execute a SQL query and return results if any."""
    cursor = conn.cursor()
    try:
        cursor.execute(query)
        try:
            results = cursor.fetchall()
            return results
        except:
            return None
    except Exception as e:
        logging.error(f"Query failed: {e}\nQuery: {query}")
        raise
    finally:
        cursor.close()

def load_raw_data(conn):
    """Load raw data from staged CSV into raw table."""
    logging.info("Starting raw data load...")
    copy_sql = """
        COPY INTO raw_financial_data
        FROM @stage/mock_financial_data.csv
        FILE_FORMAT = (FORMAT_NAME = 'csv_format')
        ON_ERROR = 'CONTINUE';
    """
    execute_query(conn, copy_sql)
    logging.info("Raw data loaded successfully.")

def transform_clean_data(conn):
    """Insert transformed/typed data into clean table."""
    logging.info("Starting data transformation and load to clean table...")
    transform_sql = """
        INSERT INTO clean_financial_data (transaction_id, customer_name, account_type, transaction_amount, currency, transaction_date)
        SELECT
            transaction_id,
            customer_name,
            account_type,
            TRY_TO_DOUBLE(transaction_amount) AS transaction_amount,
            currency,
            TRY_TO_DATE(transaction_date, 'YYYY-MM-DD') AS transaction_date
        FROM raw_financial_data;
    """
    execute_query(conn, transform_sql)
    logging.info("Data transformation and load completed.")

def run_data_quality_tests(conn):
    """Run data quality checks and log results."""
    logging.info("Running data quality tests...")
    tests = {
        "NULL transaction_id": "SELECT COUNT(*) FROM clean_financial_data WHERE transaction_id IS NULL;",
        "Duplicate transaction_id": "SELECT COUNT(*) - COUNT(DISTINCT transaction_id) FROM clean_financial_data;",
        "Negative transaction_amount": "SELECT COUNT(*) FROM clean_financial_data WHERE transaction_amount < 0;",
        "Invalid currency": "SELECT COUNT(*) FROM clean_financial_data WHERE currency NOT IN ('USD', 'EUR', 'GBP') OR currency IS NULL;",
        "Future transaction_date": "SELECT COUNT(*) FROM clean_financial_data WHERE transaction_date > CURRENT_DATE();"
    }
    for test_name, query in tests.items():
        count = execute_query(conn, query)[0][0]
        if count == 0:
            logging.info(f"Data Quality Check Passed: {test_name}")
        else:
            logging.warning(f"Data Quality Check Failed: {test_name} - {count} issues found")

def main():
    logging.info("ETL Pipeline started.")
    conn = connect_snowflake()

    try:
        load_raw_data(conn)
        transform_clean_data(conn)
        run_data_quality_tests(conn)
        logging.info("ETL Pipeline completed successfully.")
    except Exception as e:
        logging.error(f"ETL Pipeline failed: {e}")
    finally:
        conn.close()

if __name__ == "__main__":
    main()
