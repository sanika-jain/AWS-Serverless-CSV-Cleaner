# Serverless CSV Cleaning Pipeline 🚀

A serverless AWS pipeline that automatically cleans and validates CSV files to ensure high-quality data for analytics.  
Designed for **data engineering** and **cloud engineering** workflows, this project demonstrates skills in **AWS Lambda**, **S3**, and **Python** — all within the AWS Free Tier.

---

## 📋 Overview

When a CSV file is uploaded to the `raw/` folder of an S3 bucket, an **S3 event trigger** invokes a Lambda function to:

1. Validate the file location (`raw/` folder only).
2. Read and decode the CSV file using UTF-8.
3. Extract and validate headers.
4. Clean the data by removing:
   - Empty rows
   - Whitespace-only rows
   - Comma-only rows
   - Rows with missing/null values
5. Save the cleaned CSV file to the `cleaned/` folder.
6. Log errors to both the `logs/` folder in S3 and **CloudWatch**.

---

## 🏗 Architecture

S3 (raw/) → Lambda → S3 (cleaned/ + logs/)


---

## 🛠 Features

- **Automated S3 Trigger** – Processes files instantly upon upload.
- **File Location Validation** – Ensures files are processed only from the correct folder.
- **Robust Cleaning** – Handles missing fields, whitespace, and malformed rows.
- **Error Logging** – Logs detailed skipped row info to S3 and CloudWatch.
- **AWS Free Tier Friendly** – Low runtime (~305 ms) and memory usage (128 MB).

---

## ⚙️ Setup Instructions

### 1️⃣ Create S3 Bucket
Create a bucket named: my-datapipelinebucket

with the following folders:

raw/

cleaned/

logs/


### 2️⃣ Deploy Lambda Function
- Create a new Lambda function (Python 3.13 runtime).
- Copy `lambda_function.py` into the Lambda editor.
- Set:
  - **Memory:** 128 MB
  - **Timeout:** 15 seconds

### 3️⃣ Configure S3 Trigger
- In the Lambda configuration, add an **S3 trigger** for `raw/` folder uploads.

### 4️⃣ Set IAM Role
Attach these AWS managed policies:
- `AmazonS3FullAccess`
- `AWSLambdaBasicExecutionRole`

### 5️⃣ Test the Pipeline
- Upload a sample CSV file to: s3://my-datapipelinebucket/raw/
- Check:
  - `cleaned/` folder for processed CSV
  - `logs/` folder for error log









