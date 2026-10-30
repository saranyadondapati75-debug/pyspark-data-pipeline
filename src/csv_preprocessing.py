
import csv
from pathlib import Path


def split_csv_records(input_file, valid_file, malformed_file):
    valid_rows = []
    malformed_rows = []

    input_file = Path(input_file)
    valid_file = Path(valid_file)
    malformed_file = Path(malformed_file)

    try:
        with input_file.open(
            "r", newline="", encoding="utf-8"
        ) as file:
            lines = file.readlines()

        if not lines:
            raise ValueError("Input CSV file is empty.")

        header = next(csv.reader([lines[0]], strict=True))
        data_lines = lines[1:]

        for raw_record in data_lines:
            try:
                parsed = next(
                    csv.reader([raw_record], strict=True)
                )

                if len(parsed) != len(header):
                    malformed_rows.append({
                        "reason": "Incorrect column count",
                        "raw_record": raw_record.rstrip("\r\n"),
                    })
                else:
                    valid_rows.append(dict(zip(header, parsed)))

            except csv.Error as error:
                malformed_rows.append({
                    "reason": str(error),
                    "raw_record": raw_record.rstrip("\r\n"),
                })

        valid_file.parent.mkdir(parents=True, exist_ok=True)
        malformed_file.parent.mkdir(parents=True, exist_ok=True)

        with valid_file.open(
            "w", newline="", encoding="utf-8"
        ) as file:
            writer = csv.DictWriter(file, fieldnames=header)
            writer.writeheader()
            writer.writerows(valid_rows)

        with malformed_file.open(
            "w", newline="", encoding="utf-8"
        ) as file:
            writer = csv.DictWriter(
                file,
                fieldnames=["reason", "raw_record"],
            )
            writer.writeheader()
            writer.writerows(malformed_rows)

        return len(valid_rows), len(malformed_rows)

    except OSError as error:
        raise RuntimeError(
            f"CSV preprocessing failed: {error}"
        ) from error
