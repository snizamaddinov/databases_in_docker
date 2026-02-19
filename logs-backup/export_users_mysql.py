import json
from datetime import datetime, timezone

import pymysql

from config import (
    EXPORT_LOG_PATH,
    MYSQL_DB,
    MYSQL_HOST,
    MYSQL_ID_COLUMN,
    MYSQL_PASSWORD,
    MYSQL_PORT,
    MYSQL_SEASON_COLUMN,
    MYSQL_SPEND_TIME_COLUMN,
    MYSQL_USER,
    MYSQL_USER_TABLE,
    MYSQL_USERNAME_COLUMN,
    SEASON,
    USERS_JSON_PATH,
)


def utc_now():
    return datetime.now(timezone.utc).isoformat().replace("+00:00", "Z")


def reset_log():
    with open(EXPORT_LOG_PATH, "w", encoding="utf-8") as log_file:
        log_file.write(f"time={utc_now()} stage=start season={SEASON}\n")


def log_line(text):
    with open(EXPORT_LOG_PATH, "a", encoding="utf-8") as log_file:
        log_file.write(text + "\n")


def export_users():
    query = (
        f"SELECT `{MYSQL_ID_COLUMN}` AS user_id, "
        f"`{MYSQL_USERNAME_COLUMN}` AS username, "
        f"`{MYSQL_SPEND_TIME_COLUMN}` AS spend_time "
        f"FROM `{MYSQL_USER_TABLE}` "
        f"WHERE `{MYSQL_SEASON_COLUMN}` = %s"
    )

    conn = pymysql.connect(
        host=MYSQL_HOST,
        port=MYSQL_PORT,
        user=MYSQL_USER,
        password=MYSQL_PASSWORD,
        database=MYSQL_DB,
        charset="utf8mb4",
        cursorclass=pymysql.cursors.DictCursor,
    )
    try:
        with conn.cursor() as cur:
            cur.execute(query, (SEASON,))
            rows = cur.fetchall()
    finally:
        conn.close()

    users = []
    for row in rows:
        user = {
            "user_id": row.get("user_id"),
            "username": row.get("username"),
            "spend_time": row.get("spend_time"),
            "season": str(SEASON),
            "status": "new",
        }
        users.append(user)
        log_line(
            "time={time} user_id={user_id} username={username} spend_time={spend_time} status=new".format(
                time=utc_now(),
                user_id=user.get("user_id"),
                username=user.get("username"),
                spend_time=user.get("spend_time"),
            )
        )
    return users


def main():
    reset_log()
    users = export_users()
    with open(USERS_JSON_PATH, "w", encoding="utf-8") as out_file:
        json.dump(users, out_file, ensure_ascii=True, indent=2, default=str)

    summary = f"time={utc_now()} stage=done exported={len(users)} output={USERS_JSON_PATH}"
    log_line(summary)
    print(summary)


if __name__ == "__main__":
    main()
