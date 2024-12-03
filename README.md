# Monzo ETL Pipeline

A Python ETL pipeline for extracting transaction data from Monzo's API and loading it into a SQLite database stored in an S3 bucket, with logging to both local files and S3.

## Overview

This project provides an automated way to:
- Fetch transaction data from the Monzo API using OAuth2 authentication
- Transform and load the data into a SQLite database
- Upload database backups to S3
- Log pipeline operations both locally and to S3

## Key Components

### MonzoAPIClient
Handles authentication and data retrieval from the Monzo API, including:
- OAuth2 token management using DynamoDB
- Automatic token refresh
- API request handling

### MonzoSQLiteETL 
Main ETL pipeline that:
- Manages the SQLite database connection
- Transforms raw API data
- Handles database operations
- Implements logging with rotation and S3 upload
- Provides database backup functionality
 