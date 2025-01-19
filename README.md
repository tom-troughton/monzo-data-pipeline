# Monzo ETL Pipeline

A Python ETL pipeline for extracting transaction data from Monzo's API and loading it into a SQLite database stored in an S3 bucket, with logging to S3. The pipeline is hosted on AWS Lambda and automated using Eventbridge. The AWS Lambda function is created from a Docker container image.

## Overview

- Data stored in SQLite database in S3 for cost effectiveness
- Transactions data is fetched using Monzo API
- MonzoAPIClient provides interface with Monzo API
- MonzoTokenManager automates API token management 
- Data is loaded into SQLite database and transformed
- Database is uploaded back to S3
- Pipeline operations are logged and stored in S3

## Pipeline flowchart

```mermaid
graph TD;
    A[Download SQLite database from S3 - create db if first run] --> B[Fetch personal finance data from MonzoAPI];
    B --> C[Load data into bronze layer of database];
    C --> D[Transform bronze layer to silver layer];
    D --> E[Upload database back to S3];
```

## Directory Structure
```bash
monzo-data-eng/ 
├─── data/
├─── notebooks/
│   ├─── currency_ex_rate_api.ipynb
│   ├─── monzo_auth_test.ipynb
│   └─── query_sqlite_db.ipynb
├─── src/
│   ├─── extract/
│   │   └─── extract.py
│   ├─── load/
│   │   └─── load.py
│   ├─── sql/
│   │   ├─── create_bronze_layer.sql
│   │   ├─── create_gold_layer.sql
│   │   ├─── create_silver_layer.sql
│   │   └─── transform_bronze_to_silver.sql
│   ├─── transform/
│   │   └─── transform.py
│   ├─── utils/
│   │   ├─── api/
│   │   │   ├─── api_client.py
│   │   │   ├─── oauth_flow.py
│   │   │   └─── token_manager.py
│   │   ├─── initialise_database.py
│   │   ├─── logging_utils.py
│   │   └─── utils.py        
│   └─── main.py
├── tests/
│   ├── test_extract.py
│   ├── test_load.py
│   ├── test_main.py
│   └── test_transform.py
├─── .dockerignore
├─── .gitignore
├─── Dockerfile
├─── README.md
├─── docker-compose.yaml
└─── requirements.txt
```