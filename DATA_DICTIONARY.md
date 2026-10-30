# Data Dictionary

## 1. Overview

This document describes the structure, columns, data types, and validation rules for the datasets used in the PySpark Data Pipeline project.

The project uses two CSV input datasets:

- `customers.csv`
- `orders.csv`

## 2. Customers Dataset

**File:** `data/customers.csv`

**Description:** Contains customer information used to enrich order records.

| Column Name | Data Type | Description | Example |
|---|---|---|---|
| customer_id | String | Unique identifier for each customer | C001 |
| customer_name | String | Name of the customer | Anu |
| city | String | Customer's city | Hyderabad |

### Validation Rules

- `customer_id` must not be null or empty.
- Customer IDs should uniquely identify customers.
- Customer information is used to validate order customer references.

## 3. Orders Dataset

**File:** `data/orders.csv`

**Description:** Contains customer order transactions processed by the pipeline.

| Column Name | Data Type | Description | Example |
|---|---|---|---|
| order_id | String | Unique identifier for each order | O000001 |
| customer_id | String | Identifier of the customer who placed the order | C001 |
| amount | Double | Monetary value of the order | 304 |
| order_status | String | Current status of the order | pending |
| order_timestamp | Timestamp | Date and time when the order was recorded | 2026-10-08T10:00:00 |

### Validation Rules

- `order_id` must not be null or empty.
- `customer_id` must not be null or empty.
- `amount` must be numeric.
- `amount` must be greater than zero.
- `order_timestamp` must be in a valid timestamp format.
- `customer_id` must exist in the customers dataset.
- Duplicate `order_id` values are rejected.

## 4. Data Quality and Rejection Reasons

The pipeline identifies invalid records and stores them separately.

| Rejection Reason | Description |
|---|---|
| missing_order_id | Order ID is missing |
| missing_customer_id | Customer ID is missing |
| invalid_amount | Amount is not numeric |
| non_positive_amount | Amount is zero or negative |
| invalid_timestamp | Timestamp cannot be parsed |
| unknown_customer | Customer ID does not exist in customers dataset |
| duplicate_order_id | Order ID appears more than once |

## 5. Output Datasets

| Output | Description |
|---|---|
| valid_orders | Orders that pass validation |
| rejected_orders | Invalid orders with rejection reasons |
| reconciliation_report | Counts of input, valid, and rejected records |
| enriched_orders | Valid orders enriched with customer details |
| city_sales_report | Order counts and total sales grouped by city |

**Output format:** Apache Parquet

## 6. Data Processing Notes

- Input datasets are read from CSV files.
- PySpark is used for data validation and transformation.
- Valid and rejected records are stored separately.
- Customer details are joined with valid orders using `customer_id`.
- Reconciliation checks ensure that input count equals valid count plus rejected count.