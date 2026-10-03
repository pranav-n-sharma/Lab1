# Automated Weather Data Pipeline (Airflow, Snowflake, & dbt)

## 📌 Project Overview
This project implements an end-to-end automated data pipeline designed to ingest, transform, test, and track weather data for **Seattle** and **Miami**. The pipeline is orchestrated using **Apache Airflow** within a Docker container, loads raw data into a **Snowflake** data warehouse, and leverages **dbt (Data Build Tool)** for analytics engineering, data quality validation, and Slowly Changing Dimension (SCD Type 2) tracking.

---

## 🏗️ Project Architecture & Workflow
1. **Extraction & Loading**: Python tasks in Airflow extract weather data and load raw tables into Snowflake (`LAB1.raw`).
2. **Orchestration**: An Airflow DAG scheduled weekly on Sundays (`0 0 * * 0`) triggers the data load first, followed sequentially by dbt execution tasks.
3. **Transformation (`dbt run`)**: Aggregates raw daily weather metrics into weekly summaries stored in the `LAB1.TRANSFORMED` schema.
4. **Testing (`dbt test`)**: Runs automated schema tests (e.g., `not_null`) to verify data integrity.
5. **Snapshotting (`dbt snapshot`)**: Tracks historical changes in the source data over time using timestamp-based strategies.

---

## 📂 Project Directory Structure

```text
lab1/
├── analyses/
├── macros/
├── models/
│   ├── transformed/
│   │   ├── miami_weather_transform.sql
│   │   └── seattle_weather_transform.sql
│   ├── schema.yml
│   └── sources.yml
├── snapshots/
│   └── weather_snapshot.sql
├── tests/
├── target/                 # Auto-generated compiled files
└── dbt_project.yml
