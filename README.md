# PySpark Data Pipeline

## 1. Project Overview

This project implements a modular data processing pipeline using Apache PySpark.

It processes customer and order data, validates records, separates valid and rejected orders, enriches valid orders with customer information, generates city-wise sales reports, and performs reconciliation checks.

## 2. Technologies Used

- Python 3.11
- Apache PySpark 3.5.6
- Java 17
- Pytest
- CSV
- Apache Parquet

## 3. Project Structure

```text
pyspark-data-pipeline/
│
├── data/
│   ├── customers.csv
│   └── orders.csv
│
├── src/
│   ├── config.py
│   ├── generate_data.py
│   ├── order_validation.py
│   ├── pipeline_logging.py
│   ├── reconciliation.py
│   ├── transform_orders.py
│   └── validate_orders.py
│
├── tests/
│   ├── test_reconciliation.py
│   ├── test_reconciliation_report.py
│   └── test_validate_orders.py
│
├── output/
├── logs/
├── DATA_DICTIONARY.md
├── PROJECT_REPORT.md
└── README.md
```

## 4. Environment Setup

### Prerequisites

- Python 3.11
- Java 17
- Windows environment with the required Hadoop Windows utilities for local Spark execution

### Create and activate virtual environment

```powershell
python -m venv .venv
.venv\Scripts\Activate.ps1
```

### Install PySpark and Pytest

```powershell
python -m pip install pyspark==3.5.6 pytest
```

### Configure Hadoop environment for a new PowerShell session

```powershell
$env:HADOOP_HOME = "C:\hadoop"
$env:PATH = "$env:HADOOP_HOME\bin;$env:PATH"
```

## 5. Generate Synthetic Data

Generate the default sample dataset:

```powershell
python -m src.generate_data
```

Generate 100,000 normal orders:

```powershell
python -m src.generate_data --num-orders 100000
```

Use a fixed seed for reproducible data:

```powershell
python -m src.generate_data --num-orders 100000 --seed 42
```

**Note:** Data generation overwrites the existing customer and order CSV files.

## 6. Run the Data Validation Pipeline

```powershell
python -m src.validate_orders
```

The validation pipeline:

- Reads customer and order CSV files.
- Checks required fields and data quality.
- Identifies duplicate order IDs.
- Checks customer references.
- Separates valid and rejected records.
- Generates a reconciliation report.
- Writes output datasets in Parquet format.
- Records pipeline execution details in log files.

## 7. Run Data Transformation

After successful validation, execute:

```powershell
python -m src.transform_orders
```

This process:

- Reads validated orders.
- Joins orders with customer information.
- Uses a broadcast join optimization for the customer dataset.
- Generates enriched order data.
- Creates a city-wise sales report.

## 8. Run Automated Tests

```powershell
python -m pytest -v
```

The tests cover validation, duplicate records, invalid values, missing fields, empty input, and reconciliation.

## 9. Output Files

The pipeline generates Parquet datasets under the `output/` directory:

- `valid_orders`
- `rejected_orders`
- `reconciliation_report`
- `enriched_orders`
- `city_sales_report`

Execution logs are stored in the `logs/` directory.

## 10. Configuration

File paths and output locations are managed centrally in:

`src/config.py`

This makes it easier to change input and output locations without modifying the main pipeline logic.

## 11. Project Documentation

- `PROJECT_REPORT.md` — implementation details, execution results, testing, and performance observations.
- `DATA_DICTIONARY.md` — input and output dataset descriptions, columns, and validation rules.

## 12. Notes

- The synthetic data generator uses a configurable random seed.
- Pipeline output is written using overwrite mode.
- Reconciliation verifies that input records are accounted for as valid or rejected.
- Performance benchmark timings can vary depending on the machine and Spark execution environment.