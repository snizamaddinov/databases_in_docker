import json
from collections import deque
from datetime import datetime, timedelta, timezone

import pymysql
from pymongo import MongoClient


MYSQL_HOST = "127.0.0.1"
MYSQL_PORT = 3306
MYSQL_USER = "root"
MYSQL_PASSWORD = "root"
MYSQL_DB = "default"
MYSQL_USER_TABLE = "user"
MYSQL_USERNAME_COLUMN = "username"

DORIS_HOST = "127.0.0.1"
DORIS_PORT = 9030
DORIS_USER = "root"
DORIS_PASSWORD = "root"
DORIS_DB = "digital_analytics"
DORIS_TABLE = "visit_log_2526"

MONGO_URI = "mongodb://root:9cQ4ediTXPGE@10.10.4.210:27017/"
MONGO_DB = "matomo"
MONGO_COLLECTION_VISIT = "visit_log_2526"
MONGO_COLLECTION_VIDEO = "video_log_2526"

SEASON = "2526"
CUTOFF_DATE_STR = "2025-12-19"
CUTOFF_DT_UTC = datetime.strptime(CUTOFF_DATE_STR, "%Y-%m-%d").replace(tzinfo=timezone.utc)

MONGO_BATCH_SIZE = 500
MYSQL_FETCH_SIZE = 5000
DORIS_PAGE_SIZE = 2000
MATCH_WINDOW_SECONDS = 180

STATE_JSON_PATH = "backfill_state_2526.json"
LOG_FILE_PATH = "backfill_2526.log"

MAX_USERS_PER_RUN = 0
ONLY_ONE_USERNAME = "metodbox.demo10"


_log_fh = None


def log(msg):
    global _log_fh
    ts = datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S")
    line = f"{ts} {msg}"
    print(line)
    if _log_fh is None:
        _log_fh = open(LOG_FILE_PATH, "a", encoding="utf-8", buffering=1)
    _log_fh.write(line + "\n")


def mysql_connect():
    log(f"[mysql_connect] Connecting to MySQL: host={MYSQL_HOST}, port={MYSQL_PORT}, db={MYSQL_DB}, user={MYSQL_USER}")
    conn = pymysql.connect(
        host=MYSQL_HOST,
        port=MYSQL_PORT,
        user=MYSQL_USER,
        password=MYSQL_PASSWORD,
        database=MYSQL_DB,
        charset="utf8mb4",
        autocommit=True,
        connect_timeout=60,
        read_timeout=600,
        write_timeout=600,
        cursorclass=pymysql.cursors.DictCursor,
    )
    log(f"[mysql_connect] Successfully connected to MySQL")
    return conn


def doris_connect():
    log(f"[doris_connect] Connecting to Doris: host={DORIS_HOST}, port={DORIS_PORT}, db={DORIS_DB}, user={DORIS_USER}")
    conn = pymysql.connect(
        host=DORIS_HOST,
        port=DORIS_PORT,
        user=DORIS_USER,
        database=DORIS_DB,
        charset="utf8mb4",
        autocommit=True,
        connect_timeout=60,
        read_timeout=600,
        write_timeout=600,
        cursorclass=pymysql.cursors.DictCursor,
    )
    log(f"[doris_connect] Successfully connected to Doris")
    return conn


def mongo_connect():
    log(f"[mongo_connect] Connecting to MongoDB: uri={MONGO_URI[:50]}..., db={MONGO_DB}")
    client = MongoClient(
        MONGO_URI,
        serverSelectionTimeoutMS=60000,
        connectTimeoutMS=60000,
        socketTimeoutMS=600000,
    )
    log(f"[mongo_connect] Successfully connected to MongoDB")
    return client


def load_state():
    log(f"[load_state] Loading state from {STATE_JSON_PATH}")
    try:
        with open(STATE_JSON_PATH, "r", encoding="utf-8") as f:
            s = json.load(f)
        log(f"[load_state] State loaded successfully. Keys: {list(s.keys())}")
        log(f"[load_state] Processed usernames count: {len(s.get('processed_usernames', {}))}")
    except Exception as e:
        log(f"[load_state] No existing state file or error reading it: {e}. Creating new state.")
        s = {}

    s.setdefault("season", SEASON)
    s.setdefault("cutoff_date", CUTOFF_DATE_STR)
    s.setdefault("processed_usernames", {})
    s.setdefault("last_run", {})
    log(f"[load_state] State initialized with season={s['season']}, cutoff_date={s['cutoff_date']}")
    return s


def save_state(state):
    log(f"[save_state] Saving state to {STATE_JSON_PATH}")
    log(f"[save_state] State contains {len(state.get('processed_usernames', {}))} processed usernames")
    with open(STATE_JSON_PATH, "w", encoding="utf-8") as f:
        json.dump(state, f, ensure_ascii=False, indent=2)
    log(f"[save_state] State saved successfully")


def iter_usernames(mysql_conn):
    if ONLY_ONE_USERNAME:
        log(f"[iter_usernames] Using ONLY_ONE_USERNAME mode: {ONLY_ONE_USERNAME}")
        yield ONLY_ONE_USERNAME
        return

    sql = f"""
    SELECT `{MYSQL_USERNAME_COLUMN}` AS username
    FROM `{MYSQL_USER_TABLE}`
    WHERE `{MYSQL_USERNAME_COLUMN}` IS NOT NULL AND `{MYSQL_USERNAME_COLUMN}`!='' AND season = {SEASON}
    ORDER BY `{MYSQL_USERNAME_COLUMN}` ASC
    """
    log(f"[iter_usernames] Executing query to fetch usernames from MySQL")
    log(f"[iter_usernames] Query: {sql.strip()}")
    with mysql_conn.cursor() as cur:
        cur.execute(sql)
        batch_count = 0
        total_count = 0
        while True:
            rows = cur.fetchmany(MYSQL_FETCH_SIZE)
            if not rows:
                log(f"[iter_usernames] No more rows. Total usernames yielded: {total_count}")
                break
            batch_count += 1
            log(f"[iter_usernames] Fetched batch {batch_count} with {len(rows)} rows")
            for r in rows:
                u = str(r["username"]).strip()
                if u:
                    total_count += 1
                    yield u


def mongo_cursor_for_username(collection, username, start_dt_utc):
    query = {
        "uName": username,
        "serverTime": {"$gte": start_dt_utc},
    }
    projection = {
        "_id": 1,
        "Ctgry": 1,
        "Actn": 1,
        "serverTime": 1,
        "sessionId": 1,
        "connectionId": 1,
        "stoysId": 1,
        "uName": 1,
    }
    log(f"[mongo_cursor_for_username] Creating cursor for username={username}, collection={collection.name}")
    log(f"[mongo_cursor_for_username] Query: {query}")
    log(f"[mongo_cursor_for_username] Projection: {projection}")
    return collection.find(query, projection=projection).sort("serverTime", 1).batch_size(MONGO_BATCH_SIZE)


def mongo_merge_iter(visit_cursor, video_cursor):
    v_next = None
    vd_next = None

    def advance(cur):
        try:
            return next(cur)
        except StopIteration:
            return None

    v_next = advance(visit_cursor)
    vd_next = advance(video_cursor)

    while v_next is not None or vd_next is not None:
        if v_next is None:
            yield vd_next
            vd_next = advance(video_cursor)
            continue
        if vd_next is None:
            yield v_next
            v_next = advance(visit_cursor)
            continue

        v_time = v_next.get("serverTime")
        vd_time = vd_next.get("serverTime")

        if v_time <= vd_time:
            yield v_next
            v_next = advance(visit_cursor)
        else:
            yield vd_next
            vd_next = advance(video_cursor)


def normalize_event_type(ctgry, actn):
    if ctgry is None:
        ctgry = ""
    if actn is None:
        actn = ""
    result = f"{ctgry}{actn}"
    log(f"[normalize_event_type] Normalized ctgry={ctgry}, actn={actn} -> {result}")
    return result


def bson_dt_to_unix_seconds(dt):
    if dt is None:
        log(f"[bson_dt_to_unix_seconds] Input is None, returning None")
        return None
    if isinstance(dt, datetime):
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=timezone.utc)
            log(f"[bson_dt_to_unix_seconds] Added UTC timezone to naive datetime: {dt}")
        result = int(dt.timestamp())
        log(f"[bson_dt_to_unix_seconds] Converted datetime {dt} to timestamp {result}")
        return result
    try:
        result = int(dt.timestamp())
        log(f"[bson_dt_to_unix_seconds] Converted {dt} to timestamp {result}")
        return result
    except Exception as e:
        log(f"[bson_dt_to_unix_seconds] Error converting {dt}: {e}")
        return None


def doris_rows_iter(doris_conn, src_code, cutoff_ts):
    log(f"[doris_rows_iter] Starting iteration for src_code={src_code}, cutoff_ts={cutoff_ts}")
    last_ts = None
    last_id = ""

    month_floor = datetime(CUTOFF_DT_UTC.year, CUTOFF_DT_UTC.month, 1, tzinfo=timezone.utc).date().isoformat()
    log(f"[doris_rows_iter] Month floor calculated: {month_floor}")

    page_count = 0
    while True:
        page_count += 1
        sql = f"""
        SELECT
            `id`,
            `eventType`,
            `timestamp`,
            `event_month`,
            `sessionId`,
            `connectionId`
        FROM `{DORIS_TABLE}`
        WHERE
            `sourceUserId`=%s
            AND `event_month` >= %s
            AND `timestamp` >= %s
            AND (
                `sessionId` IS NULL OR `sessionId`='' OR
                `connectionId` IS NULL OR `connectionId`=''
            )
            AND (
                %s IS NULL OR
                (`timestamp` > %s OR (`timestamp` = %s AND `id` > %s))
            )
        ORDER BY `timestamp` ASC, `id` ASC
        LIMIT %s
        """
        params = [
            int(src_code),
            month_floor,
            int(cutoff_ts),
            last_ts,
            last_ts if last_ts is not None else 0,
            last_ts if last_ts is not None else 0,
            last_id,
            DORIS_PAGE_SIZE,
        ]

        log(f"[doris_rows_iter] Executing page {page_count} with params: src_code={params[0]}, month_floor={params[1]}, cutoff_ts={params[2]}, last_ts={params[3]}, last_id={params[6]}")
        with doris_conn.cursor() as cur:
            cur.execute(sql, params)
            rows = cur.fetchall()

        if not rows:
            log(f"[doris_rows_iter] No more rows found after {page_count} pages")
            break

        log(f"[doris_rows_iter] Page {page_count} fetched {len(rows)} rows")
        for r in rows:
            log(f"[doris_rows_iter] Yielding row: id={r['id']}, eventType={r.get('eventType')}, timestamp={r['timestamp']}, sessionId={r.get('sessionId')}, connectionId={r.get('connectionId')}")
            yield r
            last_ts = int(r["timestamp"])
            last_id = str(r["id"])


def apply_doris_updates(doris_conn, updates):
    log(f"[apply_doris_updates] Applying {len(updates)} updates")
    sql = f"UPDATE `{DORIS_TABLE}` SET `sessionId`=%s, `connectionId`=%s WHERE `id`=%s"
    log(f"[apply_doris_updates] SQL: {sql}")
    for i, update in enumerate(updates[:5]):  # Log first 5 updates
        log(f"[apply_doris_updates] Update {i+1}: sessionId={update[0]}, connectionId={update[1]}, id={update[2]}")
    if len(updates) > 5:
        log(f"[apply_doris_updates] ... and {len(updates) - 5} more updates")
    with doris_conn.cursor() as cur:
        cur.executemany(sql, updates)
    log(f"[apply_doris_updates] Successfully applied {len(updates)} updates")
    return len(updates)


def get_src_code_from_first_mongo_doc(username, mongo_db):
    log(f"[get_src_code] Getting src_code for username={username}")
    visit = mongo_db[MONGO_COLLECTION_VISIT]
    video = mongo_db[MONGO_COLLECTION_VIDEO]

    projection = {"_id": 1, "stoysId": 1, "serverTime": 1, "uName": 1}

    log(f"[get_src_code] Searching in visit collection")
    d1 = visit.find_one(
        {"uName": username, "serverTime": {"$gte": CUTOFF_DT_UTC}},
        projection=projection,
        sort=[("serverTime", 1)],
    )
    if d1:
        log(f"[get_src_code] Found visit doc: _id={d1.get('_id')}, stoysId={d1.get('stoysId')}, serverTime={d1.get('serverTime')}")
    if d1 and d1.get("stoysId") is not None and str(d1.get("stoysId")).strip() != "":
        src_code = str(d1.get("stoysId")).strip()
        log(f"[get_src_code] Using src_code from visit: {src_code}")
        return src_code

    log(f"[get_src_code] Searching in video collection")
    d2 = video.find_one(
        {"uName": username, "serverTime": {"$gte": CUTOFF_DT_UTC}},
        projection=projection,
        sort=[("serverTime", 1)],
    )
    if d2:
        log(f"[get_src_code] Found video doc: _id={d2.get('_id')}, stoysId={d2.get('stoysId')}, serverTime={d2.get('serverTime')}")
    if d2 and d2.get("stoysId") is not None and str(d2.get("stoysId")).strip() != "":
        src_code = str(d2.get("stoysId")).strip()
        log(f"[get_src_code] Using src_code from video: {src_code}")
        return src_code

    log(f"[get_src_code] No src_code found for username={username}")
    return ""


def backfill_username(username, mongo_db, doris_conn):
    src_code = get_src_code_from_first_mongo_doc(username, mongo_db)
    if not src_code:
        log(f"[{username}] no mongo logs with stoysId after cutoff, skip")
        return {"username": username, "src_code": "", "scanned": 0, "matched": 0, "applied": 0}

    visit = mongo_db[MONGO_COLLECTION_VISIT]
    video = mongo_db[MONGO_COLLECTION_VIDEO]

    visit_cur = mongo_cursor_for_username(visit, username, CUTOFF_DT_UTC)
    video_cur = mongo_cursor_for_username(video, username, CUTOFF_DT_UTC)
    mongo_iter = mongo_merge_iter(visit_cur, video_cur)

    cutoff_ts = int(CUTOFF_DT_UTC.timestamp())
    window = timedelta(seconds=MATCH_WINDOW_SECONDS)

    window_all = deque()
    window_by_type = {}
    used_mongo_ids = set()

    def push_mongo_doc(d):
        ts = bson_dt_to_unix_seconds(d.get("serverTime"))
        if ts is None:
            log(f"[push_mongo_doc] Skipping doc with no timestamp: _id={d.get('_id')}")
            return
        et = normalize_event_type(d.get("Ctgry"), d.get("Actn"))
        sid = d.get("sessionId")
        cid = d.get("connectionId")
        if sid is None and cid is None:
            log(f"[push_mongo_doc] Skipping doc with no sessionId/connectionId: _id={d.get('_id')}, ts={ts}")
            return
        obj = {
            "ts": ts,
            "eventType": et,
            "sessionId": sid,
            "connectionId": cid,
            "_id": str(d.get("_id")),
        }
        log(f"[push_mongo_doc] Pushing doc: _id={obj['_id']}, ts={ts}, eventType={et}, sessionId={sid}, connectionId={cid}")
        window_all.append(obj)
        if et not in window_by_type:
            window_by_type[et] = deque()
        window_by_type[et].append(obj)
        log(f"[push_mongo_doc] Window stats: total_docs={len(window_all)}, event_types={len(window_by_type)}")

    def evict_old(now_ts):
        min_ts = now_ts - int(window.total_seconds())
        evicted_count = 0
        while window_all and window_all[0]["ts"] < min_ts:
            old = window_all.popleft()
            evicted_count += 1
            log(f"[evict_old] Evicting old doc: _id={old['_id']}, ts={old['ts']}, eventType={old['eventType']}")
            et = old["eventType"]
            dq = window_by_type.get(et)
            if dq and dq[0] is old:
                dq.popleft()
                if not dq:
                    del window_by_type[et]
        if evicted_count > 0:
            log(f"[evict_old] Evicted {evicted_count} docs. Window stats: total_docs={len(window_all)}, event_types={len(window_by_type)}")

    mongo_buffer = None

    def load_until(max_ts):
        nonlocal mongo_buffer
        loaded_count = 0
        log(f"[load_until] Loading mongo docs until max_ts={max_ts}")
        while True:
            if mongo_buffer is not None:
                d = mongo_buffer
                mongo_buffer = None
                log(f"[load_until] Using buffered doc: _id={d.get('_id')}")
            else:
                try:
                    d = next(mongo_iter)
                    log(f"[load_until] Fetched next doc from mongo: _id={d.get('_id')}")
                except StopIteration:
                    log(f"[load_until] No more mongo docs. Loaded {loaded_count} docs in this call")
                    return
            ts = bson_dt_to_unix_seconds(d.get("serverTime"))
            if ts is None:
                log(f"[load_until] Skipping doc with no timestamp: _id={d.get('_id')}")
                continue
            if ts <= max_ts:
                push_mongo_doc(d)
                loaded_count += 1
            else:
                mongo_buffer = d
                log(f"[load_until] Doc exceeds max_ts ({ts} > {max_ts}), buffering. Loaded {loaded_count} docs in this call")
                return

    updates = []
    scanned = 0
    matched = 0
    applied = 0

    log(f"[{username}] start src_code={src_code}")

    for row in doris_rows_iter(doris_conn, src_code, cutoff_ts):
        scanned += 1
        doris_id = str(row["id"])
        doris_et = row.get("eventType") or ""
        doris_ts = int(row["timestamp"])
        doris_sid = row.get("sessionId")
        doris_cid = row.get("connectionId")

        max_ts = doris_ts + int(window.total_seconds())
        load_until(max_ts)
        evict_old(doris_ts)

        dq = window_by_type.get(doris_et)
        if not dq:
            log(f"[{username}] No candidates in window for eventType={doris_et}, doris_id={doris_id}")
            if scanned % 5000 == 0:
                log(f"[{username}] scanned={scanned} matched={matched} queued_updates={len(updates)}")
            continue

        log(f"[{username}] Matching doris_id={doris_id}, eventType={doris_et}, ts={doris_ts}. Found {len(dq)} candidates in window")
        best = None
        best_diff = None
        for cand in dq:
            if cand["_id"] in used_mongo_ids:
                log(f"[{username}] Candidate {cand['_id']} already used, skipping")
                continue
            diff = abs(cand["ts"] - doris_ts)
            log(f"[{username}] Candidate: _id={cand['_id']}, ts={cand['ts']}, diff={diff}, sessionId={cand.get('sessionId')}, connectionId={cand.get('connectionId')}")
            if diff <= int(window.total_seconds()):
                if best is None or diff < best_diff:
                    best = cand
                    best_diff = diff
                    log(f"[{username}] New best candidate: _id={best['_id']}, diff={best_diff}")

        if best is None:
            log(f"[{username}] No suitable match found for doris_id={doris_id}")
            if scanned % 5000 == 0:
                log(f"[{username}] scanned={scanned} matched={matched} queued_updates={len(updates)}")
            continue

        log(f"[{username}] Best match for doris_id={doris_id}: mongo_id={best['_id']}, diff={best_diff}")
        new_sid = doris_sid
        new_cid = doris_cid

        if new_sid is None or str(new_sid) == "":
            if best.get("sessionId") is not None and str(best.get("sessionId")) != "":
                new_sid = str(best.get("sessionId"))
                log(f"[{username}] Updated sessionId from None/empty to {new_sid}")
        if new_cid is None or str(new_cid) == "":
            if best.get("connectionId") is not None and str(best.get("connectionId")) != "":
                new_cid = str(best.get("connectionId"))
                log(f"[{username}] Updated connectionId from None/empty to {new_cid}")

        if (new_sid is None or str(new_sid) == "") and (new_cid is None or str(new_cid) == ""):
            log(f"[{username}] Both new_sid and new_cid are empty, skipping update for doris_id={doris_id}")
            continue

        used_mongo_ids.add(best["_id"])
        updates.append((new_sid, new_cid, doris_id))
        matched += 1
        log(f"[{username}] Queued update for doris_id={doris_id}: new_sid={new_sid}, new_cid={new_cid}")

        if len(updates) >= 300:
            applied += apply_doris_updates(doris_conn, updates)
            updates = []
            log(f"[{username}] applied_batch total_applied={applied} scanned={scanned} matched={matched}")

        if scanned % 5000 == 0:
            log(f"[{username}] scanned={scanned} matched={matched} queued_updates={len(updates)}")

    if updates:
        applied += apply_doris_updates(doris_conn, updates)
        log(f"[{username}] applied_final_batch total_applied={applied} scanned={scanned} matched={matched}")

    log(f"[{username}] done scanned={scanned} matched={matched} applied={applied}")

    return {"username": username, "src_code": src_code, "scanned": scanned, "matched": matched, "applied": applied}


def main():
    log("[main] Starting main execution")
    state = load_state()
    start_time = datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S")
    state["last_run"] = {"started_at_utc": start_time}
    log(f"[main] Run started at {start_time}")
    save_state(state)

    mysql_conn = mysql_connect()
    print("Mysql connection built")
    doris_conn = doris_connect()
    print("Doris connection built")
    mongo_client = mongo_connect()
    print("Mongo connection built")

    try:
        mongo_db = mongo_client[MONGO_DB]

        processed = state.get("processed_usernames", {})
        total_seen = 0
        total_processed = 0
        total_applied = 0

        for username in iter_usernames(mysql_conn):
            total_seen += 1
            log(f"[run] total_seen={total_seen}, username={username}")

            if username in processed:
                log(f"[run] username={username} already processed, skipping")
                continue

            total_processed += 1
            log(f"[run] processing username={username} (#{total_processed})")

            try:
                result = backfill_username(username, mongo_db, doris_conn)
                total_applied += int(result.get("applied", 0))
                log(f"[run] Result for username={username}: {result}")

                processed[username] = {
                    "processed_at_utc": datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S"),
                    "src_code": result.get("src_code", ""),
                    "scanned": int(result.get("scanned", 0)),
                    "matched": int(result.get("matched", 0)),
                    "applied": int(result.get("applied", 0)),
                }
                log(f"[run] Processed record for username={username}: {processed[username]}")
                state["processed_usernames"] = processed
                state["last_run"] = {
                    "started_at_utc": state["last_run"].get("started_at_utc", ""),
                    "updated_at_utc": datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S"),
                    "total_seen_usernames": total_seen,
                    "total_processed_usernames": total_processed,
                    "total_applied_updates": total_applied,
                }
                save_state(state)

                log(f"[run] saved state username={username} applied={result.get('applied', 0)} total_applied={total_applied}")

            except Exception as e:
                import traceback
                error_trace = traceback.format_exc()
                log(f"[run] ERROR username={username} err={e}")
                log(f"[run] ERROR traceback: {error_trace}")
                processed[username] = {
                    "processed_at_utc": datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S"),
                    "error": str(e),
                }
                log(f"[run] Error record for username={username}: {processed[username]}")
                state["processed_usernames"] = processed
                save_state(state)

            if MAX_USERS_PER_RUN and total_processed >= MAX_USERS_PER_RUN:
                log(f"[run] Reached MAX_USERS_PER_RUN limit: {MAX_USERS_PER_RUN}")
                break

        log(f"[run] finished total_seen={total_seen} total_processed={total_processed} total_applied={total_applied}")

    finally:
        log("[main] Cleaning up connections")
        try:
            mysql_conn.close()
            log("[main] MySQL connection closed")
        except Exception as e:
            log(f"[main] Error closing MySQL connection: {e}")
        try:
            doris_conn.close()
            log("[main] Doris connection closed")
        except Exception as e:
            log(f"[main] Error closing Doris connection: {e}")
        try:
            mongo_client.close()
            log("[main] MongoDB connection closed")
        except Exception as e:
            log(f"[main] Error closing MongoDB connection: {e}")
        try:
            global _log_fh
            if _log_fh:
                _log_fh.close()
                log("[main] Log file closed")
        except Exception as e:
            log(f"[main] Error closing log file: {e}")


if __name__ == "__main__":
    main()
