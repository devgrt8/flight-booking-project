# FLIGHT BOOKING PROJECT

# FLIGHT BOOKING PROJECT
# ✈️ Flight Booking Data Pipeline

[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](https://opensource.org/licenses/MIT)
[![Python 3.8+](https://img.shields.io/badge/python-3.8+-blue.svg)](https://www.python.org/downloads/)
[![Airflow](https://img.shields.io/badge/Airflow-2.0+-green.svg)](https://airflow.apache.org/)
[![GCP](https://img.shields.io/badge/GCP-Dataproc%20%7C%20BigQuery%20%7C%20Composer-orange.svg)](https://cloud.google.com/)

## 📝 Description

An end-to-end data pipeline for processing and analyzing flight booking data. This project automates the extraction, transformation, and loading (ETL) of flight booking records from Google Cloud Storage (GCS) into BigQuery using Apache Airflow, Dataproc (PySpark), and Google Cloud Composer.

### Key Workflow:
1. **Trigger**: Airflow DAG checks for new CSV files in GCS
2. **Processing**: PySpark job on Dataproc transforms and aggregates data
3. **Storage**: Results are loaded into BigQuery for analytics
4. **CI/CD**: Cloud Build automates deployment across environments (dev/prod)

## ✨ Features

- **Automated Pipeline**: Airflow DAG (`flight_booking_dataproc_bq_job`) monitors GCS for new data
- **Scalable Processing**: PySpark on Dataproc handles data transformations
- **Data Enrichment**: Adds derived features:
  - Weekend flag (`is_weekend`)
  - Lead time categorization (last-minute, short-term, long-term)
  - Booking success rate calculation
- **Analytics Ready**: Generates two insight tables:
  - **Route Insights**: Aggregates by flight route
  - **Origin Insights**: Aggregates by booking origin
- **Multi-Environment**: Supports dev and prod environments with separate configurations
- **CI/CD Pipeline**: Cloud Build automates deployment of DAGs, variables, and Spark jobs

## 🛠️ Tech Stack

| Component | Technology |
|-----------|------------|
| **Orchestration** | Apache Airflow (via Google Cloud Composer) |
| **Processing** | PySpark on Google Cloud Dataproc |
| **Storage** | Google Cloud Storage (GCS) |
| **Data Warehouse** | Google BigQuery |
| **CI/CD** | Google Cloud Build |
| **Language** | Python 3.8+ |

## 📁 Project Structure

flight-booking-project/
├── airflow_job.py # Airflow DAG definition
├── spark_job.py # PySpark ETL logic
├── cloudbuild.yaml # CI/CD pipeline configuration
├── variables/
│ ├── dev.json # Development environment variables
│ └── prod.json # Production environment variables
├── .github/
│ └── workflows/
│ └── ci_cd.yaml # GitHub Actions CI/CD (optional)
└── README.md # This file

## 🚀 Getting Started

### Prerequisites

1. **Google Cloud Platform Account** with:
   - A project with billing enabled
   - BigQuery API enabled
   - Dataproc API enabled
   - Cloud Composer API enabled

2. **Local Tools**:
   - Python 3.8+
   - Google Cloud SDK (`gcloud`)
   - Access to the GCP project