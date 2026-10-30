
import csv
import random
import argparse
from pathlib import Path

# Project data directory
DATA_DIR = Path(__file__).resolve().parent.parent / "data"


def generate_data(num_orders=100, seed=42):
    # Use a fixed seed for reproducible data
    random.seed(seed)

    DATA_DIR.mkdir(parents=True, exist_ok=True)

    customers = [
        {"customer_id": "C001", "customer_name": "Anu", "city": "Hyderabad"},
        {"customer_id": "C002", "customer_name": "Ravi", "city": "Vijayawada"},
        {"customer_id": "C003", "customer_name": "Priya", "city": "Chennai"},
        {"customer_id": "C004", "customer_name": "Rahul", "city": "Bengaluru"},
        {"customer_id": "C005", "customer_name": "Meena", "city": "Hyderabad"},
    ]

    # Save customers
    with open(DATA_DIR / "customers.csv", "w", newline="", encoding="utf-8") as file:
        writer = csv.DictWriter(
            file,
            fieldnames=["customer_id", "customer_name", "city"]
        )
        writer.writeheader()
        writer.writerows(customers)

    orders = []

    for order_number in range(1, num_orders + 1):
        customer = random.choice(customers)

        order = {
            "order_id": f"O{order_number:06d}",
            "customer_id": customer["customer_id"],
            "amount": random.randint(100, 5000),
            "order_status": random.choice(["completed", "pending"]),
            "order_timestamp": (
                f"2026-10-{random.randint(1, 28):02d}T10:00:00"
            ),
        }

        orders.append(order)

    # Add deliberately invalid examples
    invalid_orders = [
        {
            "order_id": "INVALID001",
            "customer_id": "C999",
            "amount": 600,
            "order_status": "completed",
            "order_timestamp": "2026-10-15T10:00:00",
        },
        {
            "order_id": "INVALID002",
            "customer_id": "C001",
            "amount": -100,
            "order_status": "completed",
            "order_timestamp": "2026-10-16T10:00:00",
        },
        {
            "order_id": "INVALID003",
            "customer_id": "",
            "amount": 250,
            "order_status": "pending",
            "order_timestamp": "2026-10-17T10:00:00",
        },
    ]

    orders.extend(invalid_orders)

    # Add a duplicate of the first valid order
    if orders:
        orders.append(orders[0].copy())

    # Save orders
    with open(DATA_DIR / "orders.csv", "w", newline="", encoding="utf-8") as file:
        writer = csv.DictWriter(
            file,
            fieldnames=[
                "order_id",
                "customer_id",
                "amount",
                "order_status",
                "order_timestamp",
            ]
        )
        writer.writeheader()
        writer.writerows(orders)

    print("Synthetic data generated successfully!")
    print("Customer records:", len(customers))
    print("Normal order records:", num_orders)
    print("Total order records:", len(orders))
    print("Files saved in:", DATA_DIR.resolve())


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Generate synthetic order data")
    parser.add_argument("--num-orders", type=int, default=100)
    parser.add_argument("--seed", type=int, default=42)

    args = parser.parse_args()

    if args.num_orders < 1:
        parser.error("--num-orders must be at least 1")

    generate_data(num_orders=args.num_orders, seed=args.seed)

