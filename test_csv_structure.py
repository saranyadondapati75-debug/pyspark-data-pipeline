
import csv

file_path = "data/malformed_test.csv"

valid_rows = []
malformed_rows = []

try:
    with open(file_path, "r", newline="", encoding="utf-8") as file:
        reader = csv.DictReader(file, strict=True)

        while True:
            try:
                row = next(reader)
            except StopIteration:
                break
            except csv.Error as error:
                malformed_rows.append(str(error))
                break

            if None in row or any(value is None for value in row.values()):
                malformed_rows.append(
                    f"Incorrect column count: {row}"
                )
            else:
                valid_rows.append(row)

except OSError as error:
    print("File error:", error)

print("Valid rows:", len(valid_rows))
print("Malformed rows:", len(malformed_rows))

for row in malformed_rows:
    print("Malformed record:", row)
