import csv
import json

import pymysql

from config import MYSQL_DB, MYSQL_HOST, MYSQL_PASSWORD, MYSQL_PORT, MYSQL_USER

INPUT_CSV = "users_2526_comparison.csv"
OUTPUT_CSV = "users_2526_comparison_with_roles.csv"
BATCH_SIZE = 1000

def main():
    with open(INPUT_CSV, "r", encoding="utf-8", newline="") as f:
        reader = csv.DictReader(f, delimiter=";")
        rows = list(reader)

    user_ids = []
    for row in rows:
        uid = row.get("uid", "").strip()
        if uid:
            user_ids.append(int(uid))

    if not user_ids:
        print("No user ids found in CSV.")
        return

    conn = pymysql.connect(
        host=MYSQL_HOST,
        port=MYSQL_PORT,
        user=MYSQL_USER,
        password=MYSQL_PASSWORD,
        database=MYSQL_DB,
        charset="utf8mb4",
        cursorclass=pymysql.cursors.DictCursor,
    )

    role_by_id = {}

    try:
        with conn.cursor() as cur:
            for i in range(0, len(user_ids), BATCH_SIZE):
                batch = user_ids[i:i + BATCH_SIZE]
                placeholders = ",".join(["%s"] * len(batch))
                query = f"SELECT id, roles FROM user WHERE id IN ({placeholders})"
                
                cur.execute(query, batch)
                db_rows = cur.fetchall()

                for row in db_rows:
                    roles = row.get("roles")
                    role = ""
                    if roles:
                        role = json.loads(roles)[0]
                    role_by_id[str(row["id"])] = role
    finally:
        conn.close()

    fieldnames = list(rows[0].keys()) + ["role"]
    for row in rows:
        row["role"] = role_by_id.get(row.get("uid", "").strip(), "")

    with open(OUTPUT_CSV, "w", encoding="utf-8", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames, delimiter=";")
        writer.writeheader()
        writer.writerows(rows)

    print(f"Done: {OUTPUT_CSV}")


if __name__ == "__main__":
    main()