# STEDI Data Lakehouse Project

## Overview

This project implements a data lakehouse architecture on AWS using S3, Glue, and Athena.
It processes IoT sensor data and customer data across three layers: Landing, Trusted, and Curated.

The pipeline enforces data privacy rules, removes invalid records, and prepares datasets for machine learning.

---

## Architecture

Landing → Trusted → Curated → Machine Learning

---

## Technologies

* AWS S3
* AWS Glue (Visual ETL + PySpark)
* AWS Athena
* SQL

---

## Data Pipeline

### Landing Zone

Raw JSON data ingested into S3:

* `customer_landing`
* `accelerometer_landing`
* `step_trainer_landing`

---

### Trusted Zone

Data is cleaned and filtered:

* `customer_trusted`

  * Filters users with valid research consent (`sharewithresearchasofdate IS NOT NULL`)

* `accelerometer_trusted`

  * Inner join with `customer_trusted` using email
  * Keeps only valid users' sensor data

* `step_trainer_trusted`

  * Filters records using valid `serialnumber` from curated customers
  * Prevents data explosion and duplication

---

### Curated Zone

Business-ready datasets:

* `customer_curated`

  * Deduplicated by email

* `machine_learning_curated`

  * Joins `step_trainer_trusted` with `accelerometer_trusted`
  * Combines motion sensor data for ML use cases

---

## Validation Results (Athena)

| Table                    | Rows  |
| ------------------------ | ----- |
| customer_landing         | 956   |
| accelerometer_landing    | 81273 |
| step_trainer_landing     | 28680 |
| customer_trusted         | 482   |
| accelerometer_trusted    | 40981 |
| step_trainer_trusted     | 14460 |
| customer_curated         | 482   |
| machine_learning_curated | 43681 |

---

## Key Decisions

* Used **SQL Query nodes** in Glue for more predictable joins
* Used **Data Catalog sources** instead of raw S3 when necessary
* Applied **deduplication early** to avoid data explosion
* Recreated tables and S3 data when debugging to avoid append issues

---

## Project Structure

```bash
stedi-lakehouse/
│
├── glue_jobs/
├── sql/
├── screenshots/
└── README.md
```

---

## Key Learnings

* Handling schema inconsistencies in AWS Glue
* Avoiding duplicated data due to S3 append behavior
* Debugging join cardinality issues
* Choosing between Glue visual nodes and SQL transforms

---

## Author

Bruno Battaglia
