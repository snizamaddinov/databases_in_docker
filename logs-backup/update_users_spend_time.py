import json
import os
from datetime import datetime, timezone

import pymysql

from config import (
    MYSQL_DB,
    MYSQL_HOST,
    MYSQL_ID_COLUMN,
    MYSQL_PASSWORD,
    MYSQL_PORT,
    MYSQL_SEASON_COLUMN,
    MYSQL_SPEND_TIME_COLUMN,
    MYSQL_USER,
    MYSQL_USER_TABLE,
    SEASON,
    UPDATE_LOG_PATH,
    USERS_JSON_PATH,
)


def utc_now():
    return datetime.now(timezone.utc).isoformat().replace("+00:00", "Z")

def reset_log():
    with open(UPDATE_LOG_PATH, "w", encoding="utf-8") as log_file:
        log_file.write(f"time={utc_now()} stage=start json={USERS_JSON_PATH} season={SEASON}\n")

def log_line(text):
    with open(UPDATE_LOG_PATH, "a", encoding="utf-8") as log_file:
        log_file.write(text + "\n")


def load_users():
    with open(USERS_JSON_PATH, "r", encoding="utf-8") as input_file:
        return json.load(input_file)


def update_spend_times(users):
    update_sql = (
        f"UPDATE `{MYSQL_USER_TABLE}` "
        f"SET `{MYSQL_SPEND_TIME_COLUMN}` = %s "
        f"WHERE `{MYSQL_ID_COLUMN}` = %s AND `{MYSQL_SEASON_COLUMN}` = %s"
    )

    conn = pymysql.connect(
        host=MYSQL_HOST,
        port=MYSQL_PORT,
        user=MYSQL_USER,
        password=MYSQL_PASSWORD,
        database=MYSQL_DB,
        charset="utf8mb4",
        cursorclass=pymysql.cursors.DictCursor,
        autocommit=False,
    )

    updated = 0
    skipped = 0
    failed = 0

    try:
        with conn.cursor() as cur:
            for user in users:
                user_id = user.get("user_id")
                new_spend_time = user.get("new_spend_time_seconds")
                spend_value = int(new_spend_time)

                try:
                    cur.execute(update_sql, (spend_value, user_id, SEASON))
                    conn.commit()
                    if cur.rowcount > 0:
                        updated += cur.rowcount
                        log_line(
                            "time={time} action=update user_id={user_id} spend_time={spend_time} affected={affected}".format(
                                time=utc_now(),
                                user_id=user_id,
                                spend_time=spend_value,
                                affected=cur.rowcount,
                            )
                        )
                    else:
                        skipped += 1
                        log_line(
                            "time={time} action=skip user_id={user_id} reason=no-row-updated".format(
                                time=utc_now(),
                                user_id=user_id,
                            )
                        )
                except Exception as exc:
                    conn.rollback()
                    failed += 1
                    log_line(
                        "time={time} action=error user_id={user_id} error={error}".format(
                            time=utc_now(),
                            user_id=user_id,
                            error=str(exc),
                        )
                    )
    finally:
        conn.close()

    return updated, skipped, failed


def main():
    reset_log()
    users = load_users()
    updated, skipped, failed = update_spend_times(users)
    summary = (
        f"time={utc_now()} stage=done updated={updated} skipped={skipped} failed={failed} "
        f"json={USERS_JSON_PATH}"
    )
    log_line(summary)
    print(summary)


if __name__ == "__main__":
    main()
