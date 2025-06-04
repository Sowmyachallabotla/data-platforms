-- dbt_project/models/transformation.sql

{{ config(materialized='table') }}

SELECT
    transaction_id,
    customer_name,
    account_type,
    TRY_CAST(transaction_amount AS FLOAT) AS transaction_amount,
    currency,
    TRY_CAST(transaction_date AS DATE) AS transaction_date
FROM {{ source('moody_data', 'raw_financial_data') }}
