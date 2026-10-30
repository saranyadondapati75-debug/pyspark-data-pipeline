# Data Pipeline Architecture

## 1. Overview

The project uses a modular PySpark architecture to process customer and order data. Each module has a separate responsibility, making the pipeline easier to maintain and test.

## 2. Architecture Flow

```text
       Synthetic Data Generator
                  |
                  v
          Input CSV Files
          /             \
         v               v
   customers.csv     orders.csv
         \               /
          \             /
           v           v
          Data Validation
                  |
          +-------+-------+
          |               |
          v               v
     Valid Orders    Rejected Orders
          |               |
          v               v
     Parquet Output   Rejection Reasons
          |
          v
    Customer Enrichment
          |
          v
     Enriched Orders
          |
          v
    City-wise Sales Report

    Reconciliation Report
    Pipeline Logs
```

## 3. Main Components

| Component | Responsibility |
|---|---|
| `generate_data.py` | Generates reproducible synthetic customer and order data |
| `config.py` | Manages input and output paths |
| `order_validation.py` | Applies order data quality and business validation rules |
| `validate_orders.py` | Runs the validation pipeline and writes valid, rejected, and reconciliation outputs |
| `pipeline_logging.py` | Creates run IDs and records execution logs |
| `reconciliation.py` | Compares input, valid, and rejected record counts |
| `transform_orders.py` | Enriches valid orders and generates city-wise sales reports |

## 4. Processing Stages

### Stage 1: Data Generation

Synthetic customer and order records are generated using a configurable random seed.

### Stage 2: Data Ingestion

The pipeline reads customer and order CSV files using PySpark.

### Stage 3: Data Validation

Orders are checked for missing fields, invalid amounts, invalid timestamps, unknown customers, and duplicate order IDs.

### Stage 4: Data Separation

Valid records and rejected records are written separately in Parquet format.

### Stage 5: Reconciliation

The pipeline verifies:

`Input Count = Valid Count + Rejected Count`

### Stage 6: Data Transformation

Valid orders are joined with customer information. The pipeline generates enriched orders and city-wise sales aggregates.

### Stage 7: Logging

Execution details, run IDs, record counts, and errors are recorded in log files.

## 5. Storage

| Data | Storage Location |
|---|---|
| Input customers | `data/customers.csv` |
| Input orders | `data/orders.csv` |
| Valid orders | `output/valid_orders` |
| Rejected orders | `output/rejected_orders` |
| Reconciliation report | `output/reconciliation_report` |
| Enriched orders | `output/enriched_orders` |
| City sales report | `output/city_sales_report` |
| Execution logs | `logs/` |

## 6. Design Considerations

- Modular Python code separates pipeline responsibilities.
- Centralized configuration makes file paths easier to manage.
- Rejected records retain reasons for data-quality failures.
- Parquet is used for processed output.
- Reconciliation helps identify missing or unaccounted-for records.
- Broadcast joins are used as a performance optimization for customer enrichment.