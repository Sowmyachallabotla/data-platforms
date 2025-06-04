import pandas as pd
import boto3
from faker import Faker
import random
import os

# ----------------------------
# Configuration
# ----------------------------
NUM_RECORDS = 100
CSV_FILENAME = "financial_data.csv"
S3_BUCKET_NAME = "data-bucket"  
_KEY = f"raw-data/{CSV_FILENAME}"        
# ----------------------------
# Generate mock financial data
# ----------------------------
def generate_mock_data(n):
    fake = Faker()
    data = []
    for _ in range(n):
        record = {
            "transaction_id": fake.uuid4(),
            "customer_name": fake.name(),
            "account_type": random.choice(["Savings", "Checking", "Credit"]),
            "transaction_amount": round(random.uniform(10.0, 10000.0), 2),
            "currency": "USD",
            "transaction_date": fake.date_between(start_date='-1y', end_date='today')
        }
        data.append(record)
    return pd.DataFrame(data)

# ----------------------------
# Save data to local CSV
# ----------------------------
def save_to_csv(df, filename):
    df.to_csv(filename, index=False)
    print(f"✅ Data saved to: {filename}")

# ----------------------------
# Upload CSV to AWS S3
# ----------------------------
def upload_to_s3(filename, bucket, key):
    try:
        s3 = boto3.client("s3")
        s3.upload_file(Filename=filename, Bucket=bucket, Key=key)
        print(f"✅ File uploaded to S3: s3://{bucket}/{key}")
    except Exception as e:
        print(f"❌ Failed to upload to S3: {e}")

# ----------------------------
# Main logic
# ----------------------------
def main():
    print("🚀 Generating mock financial data...")
    df = generate_mock_data(NUM_RECORDS)

    print("💾 Saving CSV locally...")
    save_to_csv(df, CSV_FILENAME)

    if os.getenv("AWS_ACCESS_KEY_ID"):
        print("☁️ Uploading to S3...")
        upload_to_s3(CSV_FILENAME, S3_BUCKET_NAME, S3_KEY)
    else:
        print("⚠️ AWS credentials not found. Skipping S3 upload.")

if __name__ == "__main__":
    main()
