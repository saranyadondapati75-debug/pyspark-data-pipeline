
def validate_input_schema(customers, orders):
    """Check that input DataFrames contain all required columns."""

    required_customer_columns = {
        "customer_id",
        "customer_name",
        "city",
    }

    required_order_columns = {
        "order_id",
        "customer_id",
        "amount",
        "order_status",
        "order_timestamp",
    }

    actual_customer_columns = set(customers.columns)
    actual_order_columns = set(orders.columns)

    missing_customer_columns = (
        required_customer_columns - actual_customer_columns
    )
    missing_order_columns = (
        required_order_columns - actual_order_columns
    )

    if missing_customer_columns or missing_order_columns:
        errors = []

        if missing_customer_columns:
            errors.append(
                f"Customers file is missing columns: "
                f"{sorted(missing_customer_columns)}"
            )

        if missing_order_columns:
            errors.append(
                f"Orders file is missing columns: "
                f"{sorted(missing_order_columns)}"
            )

        raise ValueError("Input schema validation failed. " + " ".join(errors))
