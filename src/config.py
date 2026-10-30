
from pathlib import Path

# Project root directory
BASE_DIR = Path(__file__).resolve().parent.parent

# Input files
CUSTOMERS_FILE = BASE_DIR / "data" / "customers.csv"
ORDERS_FILE = BASE_DIR / "data" / "orders.csv"

# Output directories
OUTPUT_DIR = BASE_DIR / "output"

VALID_ORDERS_PATH = OUTPUT_DIR / "valid_orders"
REJECTED_ORDERS_PATH = OUTPUT_DIR / "rejected_orders"
MALFORMED_ORDERS_PATH = OUTPUT_DIR / "malformed_orders"
RECONCILIATION_REPORT_PATH = OUTPUT_DIR / "reconciliation_report"

ENRICHED_ORDERS_PATH = OUTPUT_DIR / "enriched_orders"
CITY_SALES_REPORT_PATH = OUTPUT_DIR / "city_sales_report"
