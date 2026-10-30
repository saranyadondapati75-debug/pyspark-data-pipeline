
import csv

from src.csv_preprocessing import split_csv_records


def test_malformed_csv_record_is_quarantined(tmp_path):
    input_file = tmp_path / "orders.csv"
    clean_file = tmp_path / "clean.csv"
    malformed_file = tmp_path / "malformed.csv"

    input_file.write_text(
        "order_id,customer_id,amount,order_status,order_timestamp\n"
        "O100,C001,100,completed,2026-10-30T10:00:00\n"
        'O101,C001,200,completed,"2026-10-30T11:00:00\n'
        "O102,C002,300,pending,2026-10-30T12:00:00\n",
        encoding="utf-8",
    )

    valid_count, malformed_count = split_csv_records(
        input_file, clean_file, malformed_file
    )

    assert valid_count == 2
    assert malformed_count == 1

    with clean_file.open(encoding="utf-8", newline="") as file:
        clean_rows = list(csv.DictReader(file))

    with malformed_file.open(encoding="utf-8", newline="") as file:
        malformed_rows = list(csv.DictReader(file))

    assert [row["order_id"] for row in clean_rows] == ["O100", "O102"]
    assert "O101" in malformed_rows[0]["raw_record"]
    assert malformed_rows[0]["reason"]


def test_wrong_column_count_is_quarantined(tmp_path):
    input_file = tmp_path / "orders.csv"
    clean_file = tmp_path / "clean.csv"
    malformed_file = tmp_path / "malformed.csv"

    input_file.write_text(
        "order_id,customer_id,amount,order_status,order_timestamp\n"
        "O100,C001,100,completed,2026-10-30T10:00:00\n"
        "O101,C001,200,completed\n",
        encoding="utf-8",
    )

    valid_count, malformed_count = split_csv_records(
        input_file, clean_file, malformed_file
    )

    assert valid_count == 1
    assert malformed_count == 1
