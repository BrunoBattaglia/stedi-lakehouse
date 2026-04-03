# STEDI Data Lakehouse Project

## Overview

This project builds a data lakehouse using AWS Glue, S3, and Athena.
The goal is to process IoT and customer data across Landing, Trusted, and Curated zones.

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

* customer_landing
* accelerometer_landing
* step_trainer_landing

---

### Trusted Zone

Data cleaned and filtered:

* customer_trusted (consent applied)
* accelerometer_trusted (filtered by valid users)
* step_trainer_trusted (joined with valid customers)

---

### Curated Zone

Business-ready datasets:

* customer_curated (deduplicated customers)
* machine_learning_curated (joined sensor datasets)

---

## Validation Results

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

## Key Learnings

* Handling schema issues in AWS Glue
* Avoiding data duplication (S3 append behavior)
* Debugging joins and cardinality problems
* Using SQL vs Glue visual nodes effectively

---

## Author

Bruno Battaglia
