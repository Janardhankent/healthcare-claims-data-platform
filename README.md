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

```text
data/error/claims_invalid/claims_invalid.csv

