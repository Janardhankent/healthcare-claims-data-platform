# Healthcare Claims Data Platform

## Project Overview

This project is an end-to-end healthcare claims data engineering platform. It simulates a real-world healthcare claims processing pipeline using Python, PySpark, Azure Data Lake, Databricks, Snowflake, and Power BI.

The project is being built step by step. First, the pipeline is developed locally using sample healthcare data. Later, the same logic will be moved to cloud tools such as Azure Data Lake, Databricks, Snowflake, and Power BI.

---

## Business Use Case

Healthcare organizations process thousands of insurance claims from patients and providers. These claims need to be validated, cleaned, analyzed, and reported for business decision-making.

This project helps answer questions such as:

- How many claims were submitted?
- What is the total claim amount?
- What is the total paid amount?
- What is the denial rate?
- Which providers have the highest claim volume?
- Which diagnosis codes are most common?
- Which claims are denied?
- What is the monthly claim trend?

---

## Architecture

```text
CSV / API / Kafka
        ↓
Raw Data Layer
        ↓
Bronze Layer
        ↓
Silver Layer
        ↓
Gold Layer
        ↓
Snowflake
        ↓
Power BI Dashboard