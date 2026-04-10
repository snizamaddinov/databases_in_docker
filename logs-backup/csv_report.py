import json
import csv

INPUT_JSON = "users_2526_comparison.json"
OUTPUT_CSV = "users_2526_comparison.csv"

headers = [
    "uid",
    "username",
    "user_spend_time",
    "60 saniye",
    "300 saniye",
    "600 saniye",
    "Socket ile 60",
    "Socket ile 300",
    "Socket ile 600",
]

with open(INPUT_JSON, "r", encoding="utf-8") as f:
    data = json.load(f)

users = data.get("users", [])

with open(OUTPUT_CSV, "w", newline="", encoding="utf-8") as f:
    writer = csv.writer(f)
    writer.writerow(headers)

    for u in users:
        m1 = u.get("comparisons", {}).get("method_1", {})
        m2 = u.get("comparisons", {}).get("method_2", {})

        row = [
            u.get("user_id"),
            u.get("username"),
            u.get("source_spend_time"),
            m1.get("60", {}).get("total_seconds"),
            m1.get("300", {}).get("total_seconds"),
            m1.get("600", {}).get("total_seconds"),
            m2.get("60", {}).get("total_seconds"),
            m2.get("300", {}).get("total_seconds"),
            m2.get("600", {}).get("total_seconds"),
        ]
        writer.writerow(row)

print(f"Created: {OUTPUT_CSV}")
