# PySpark Data Pipeline – Project Report

## 1. Project Overview

This project implements a modular data pipeline using Python and Apache PySpark. It processes customer and order data, validates records, separates valid and rejected records, enriches orders with customer information, and generates city-wise sales reports.

The pipeline supports reproducible synthetic data generation and large-scale processing.

## 2. Technologies Used

- Python 3.11
- Apache PySpark 3.5.6
- Java 17
- CSV for input data
- Parquet for output data
- Pytest for unit testing
- PowerShell and Visual Studio Code

## 3. Project Architecture

The pipeline follows these stages:

1. Generate synthetic customer and order data.
2. Read customer and order CSV files.
3. Validate order records.
4. Separate valid and rejected records.
5. Generate reconciliation reports.
6. Enrich valid orders with customer information.
7. Calculate city-wise sales.
8. Save processed datasets in Parquet format.

## 4. Data Validation

The pipeline checks for:

- Missing order IDs.
- Missing customer IDs.
- Invalid order amounts.
- Non-positive amounts.
- Invalid timestamps.
- Unknown customer references.
- Duplicate order IDs.

Invalid records are separated from valid records with rejection reasons.

## 5. Synthetic Data Generation

A fixed random seed of 42 is used to generate reproducible synthetic data.

The generator supports configurable record counts, including 100,000 or more orders.

Large-scale test:
- Customers: 5
- Normal orders: 100,000
- Invalid examples: 3
- Duplicate example: 1
- Total input orders: 100,004

## 6. Pipeline Execution Results

Large-scale validation results:

| Metric | Result |
|---|---:|
| Input orders | 100,004 |
| Valid orders | 99,999 |
| Rejected orders | 5 |
| Reconciliation | Passed |

The reconciliation condition is:

100,004 = 99,999 + 5

## 7. Data Transformation Results

Valid orders are joined with customer data to produce enriched order records.

The city-wise sales report contains:

| City | Order count | Total sales |
|---|---:|---:|
| Bengaluru | 19,962 | 51,020,889 |
| Chennai | 20,079 | 51,227,265 |
| Hyderabad | 40,041 | 102,350,243 |
| Vijayawada | 19,917 | 50,833,252 |
| Total | 99,999 | 255,431,649 |

## 8. Performance Benchmark

A normal join was compared with an explicit Spark broadcast join.

| Join approach | Execution time |
|---|---:|
| Normal join | 0.90 seconds |
| Broadcast join | 0.20 seconds |

Measured execution-time reduction: 77.41%.

The broadcast join was tested because the customer dataset is small compared with the orders dataset.

These are initial benchmark results from one run. Repeated measurements and physical execution plan inspection are required to establish a reliable performance improvement.

## 9. Reconciliation and Logging

The pipeline records:

- Unique run ID.
- Pipeline start and end timestamps.
- Input record count.
- Valid record count.
- Rejected record count.
- Execution duration.
- Reconciliation status.
- Failure details when an exception occurs.

The reconciliation report is stored in Parquet format.

## 10. Testing

The project includes unit tests for validation and reconciliation.

Previously executed test results:
- 11 tests passed.

Test cases cover valid records, missing fields, invalid amounts, invalid timestamps, unknown customers, duplicate IDs, and empty batches.

## 11. Idempotency

The pipeline uses Parquet output in overwrite mode.

Repeated executions with the same input replace previous output datasets rather than appending duplicate records.

## 12. Conclusion

The project demonstrates a modular PySpark data pipeline with synthetic data generation, validation, rejection handling, reconciliation, enrichment, aggregation, logging, testing, and performance benchmarking.

The pipeline successfully processed more than 100,000 input orders and generated reconciled output datasets.