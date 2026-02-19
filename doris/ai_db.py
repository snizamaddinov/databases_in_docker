import json
import pymysql
from datetime import datetime, timedelta

SOURCE_DB = {
    "host": "metodbox-ai-mysql.metodbox.local",
    "port": 3306,
    "user": "metodbox",
    "password": "sutA96huhawUdA6gA9hU",
    "database": "metodbox-ai",
    "charset": "utf8mb4",
}

DEST_DB = {
    "host": "mbx-ai-db.metodbox.local",
    "port": 3306,
    "user": "metodbox",
    "password": "q9tB7ivhABPzqKEdWr2ijFUT",
    "database": "default",
    "charset": "utf8mb4",
}

SOURCE_TABLE = "outsource_incoming_data"
DEST_TABLE = "outsource_incoming_data"

BATCH_SIZE = 500
SEASON_VALUE = "2526"

TYPE_CONTENT = 103
TYPE_TAG = 102

ACTION_CREATE_OR_UPDATE = 201
ACTION_DELETE = 202


# Custom sort order for type field
TYPE_ORDER = [
    'ROLE',
    'SEASON',
    'CATALOG',
    'BRANCH',
    'GRADE',
    'SUBJECT',
    'CHAPTER',
    'LESSON',
    'SUB_LESSON',
    'ATTAINMENT',
    'SUB_ATTAINMENT',
    'CONTENT_BOOK',
    'CONTENT_VIDEO',
]

# Mapping from group_id to group_slug
GROUP_ID_TO_SLUG = {
    5: 'SEASON',
    6: 'GRADE',
    7: 'SUBJECT',
    8: 'CHAPTER',
    9: 'LESSON',
    11: 'SUB_LESSON',
    14: 'ATTAINMENT',
    15: 'SUB_ATTAINMENT',
    17: 'CATALOG',
    18: 'BRANCH',
    100: 'TEST',
    110: 'TEST_QUESTION',
    800: 'CONTENT_BOOK',
    900: 'CONTENT_VIDEO',
}

def process_input_json(input_str):
    if not input_str:
        return input_str
    
    try:
        input_obj = json.loads(input_str)
        
        if 'tags' in input_obj and isinstance(input_obj['tags'], list):
            for tag in input_obj['tags']:
                if isinstance(tag, dict):
                    if tag.get('group_slug') is None:
                        group_id = tag.get('group_id')
                        if group_id is not None and group_id in GROUP_ID_TO_SLUG:
                            tag['group_slug'] = GROUP_ID_TO_SLUG[group_id]
                        else:
                            tag['group_slug'] = 'CATALOG'
        
        return json.dumps(input_obj, ensure_ascii=False)
    except (json.JSONDecodeError, TypeError):
        return input_str

def main():
    src = pymysql.connect(**SOURCE_DB, cursorclass=pymysql.cursors.DictCursor)
    dst = pymysql.connect(**DEST_DB, cursorclass=pymysql.cursors.DictCursor)

    insert_sql = f"""
        INSERT INTO {DEST_TABLE}
        (type, action, status, input, output, retry_count, transaction_id, created_at, updated_at, season)
        VALUES
        (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
    """

    offset = 0
    total_inserted = 0
    row_counter = 0
    base_time = datetime(2025, 1, 1, 0, 0, 0)

    with src.cursor() as scur, dst.cursor() as dcur:
        while True:
            type_order_str = ", ".join([f"'{t}'" for t in TYPE_ORDER])
            select_sql = f"""
                SELECT type, action, input, transaction_id
                FROM {SOURCE_TABLE}
                WHERE season = %s AND status = 3
                ORDER BY FIELD(type, {type_order_str}), id
                LIMIT {BATCH_SIZE} OFFSET {offset}
            """
            scur.execute(select_sql, (SEASON_VALUE,))
            rows = scur.fetchall()
            if not rows:
                break

            batch = []
            for r in rows:
                created_at = base_time + timedelta(seconds=row_counter)
                row_counter += 1

                batch.append((
                    r["type"],
                    r["action"],
                    1,
                    process_input_json(r["input"]),
                    "{}",
                    0,
                    r["transaction_id"],
                    created_at,
                    created_at,
                    SEASON_VALUE,
                ))

            dcur.executemany(insert_sql, batch)
            dst.commit()

            total_inserted += len(batch)
            offset += BATCH_SIZE
            print(f"Inserted {len(batch)} rows | total_inserted={total_inserted} | offset={offset}")

    src.close()
    dst.close()

if __name__ == "__main__":
    main()
