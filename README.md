# Healthcare Claims Data Platform

## Project Overview

This project is an end-to-end healthcare claims data engineering platform. It simulates a real-world healthcare claims processing pipeline using Python, PySpark, Azure Data Lake, Databricks, Snowflake, and Power BI.

## Source Data

The project uses the following datasets:

- Patients
- Providers
- Claims
- Payments
- Claim Status History

## Architecture

CSV/API/Kafka → Data Lake → PySpark/Databricks → Snowflake → Power BI

## Current Progress

- Created project folder structure
- Generated sample healthcare claims data
- Added raw CSV files

## Bronze Layer

The Bronze layer stores raw ingested healthcare claims data in Parquet format.

### Source Files

- patients.csv
- providers.csv
- claims.csv
- payments.csv
- claim_status_history.csv

### Bronze Metadata Columns

- ingestion_timestamp
- source_system
- source_file_name
- bronze_table_name

### Bronze Processing

Raw CSV files are read using PySpark and written to the `data/bronze` folder as Parquet files.

### Bronze Output Tables

- bronze/patients
- bronze/providers
- bronze/claims
- bronze/payments
- bronze/claim_status_history

## Silver Layer

The Silver layer contains cleaned and validated healthcare claims data.

### Silver Processing Logic

The Silver pipeline reads Bronze data and performs the following transformations:

- Removes duplicate records
- Trims extra spaces from string columns
- Standardizes gender, state, status, and diagnosis code values
- Converts date columns to proper date format
- Converts claim amount and paid amount to numeric format
- Validates claim records
- Separates invalid claim records into the error folder

### Silver Output Tables

- patients_clean
- providers_clean
- claims_clean
- payments_clean
- claim_status_history_clean

### Error Output

Invalid claim records are written to:


data/error/claims_invalid/claims_invalid.csv

### Gold Layer

The Gold layer contains business-ready analytics tables created from the cleaned Silver data.

### Gold Processing Logic

The Gold pipeline reads cleaned Silver data and creates dimension, fact, and summary tables for reporting and dashboarding.

### Gold Tables

- dim_patient
- dim_provider
- fact_claims
- claim_summary_monthly
- provider_performance_summary
- denial_summary
- payment_summary

### Main Fact Table

The `fact_claims` table joins claims, patients, providers, and payment data to create a final analytics-ready claims table.

Important columns include:

- claim_id
- patient_id
- provider_id
- claim_date
- diagnosis_code
- procedure_code
- claim_amount
- status
- patient_state
- patient_gender
- provider_name
- specialty
- provider_state
- total_paid_amount
- payment_count
- claim_balance_amount
- is_denied
- is_approved

### Summary Tables

The Gold layer also creates aggregated reporting tables:

| Table | Purpose |
|---|---|
| claim_summary_monthly | Monthly claim trend and denial rate |
| provider_performance_summary | Provider-level claim amount, payment, and denial analysis |
| denial_summary | Denied claims by diagnosis code, specialty, and provider state |
| payment_summary | Payment and balance summary by claim status |

### Current Local Output

For local Windows development, Gold tables are written as CSV files under:

data/gold/

## SQL Analytics Queries

The project includes SQL analytics queries for reporting and dashboard development.

SQL file:

sql/analytics_queries.sql