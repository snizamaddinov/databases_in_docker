import json
import os
from datetime import datetime, timezone

from pymongo import MongoClient

from config import (
    COMPARE_LOG_PATH,
    COMPARE_OUTPUT_PATH,
    EVENTS_COLLECTION,
    METHOD2_SWITCH_AT,
    MIN_CREATED_AT,
    MONGO_DB,
    MONGO_URI,
    PAGE_SIZE,
    THRESHOLD_VALUES,
    USERS_JSON_PATH,
    VIDEO_COLLECTION,
    VISIT_COLLECTION,
)
from spend_time_methods_core import (
    calculate_connect_disconnect,
    calculate_method1,
    fmt_ts,
    normalize_event,
    parse_dt,
    parse_threshold_values,
    split_before_date,
)


def utc_now():
    return datetime.now(timezone.utc).isoformat().replace("+00:00", "Z")



def reset_log():
    with open(COMPARE_LOG_PATH, "w", encoding="utf-8") as log_file:
        log_file.write(
            "time={time} stage=start users_json={users_json} output={output} visit={visit} video={video} events={events}\n".format(
                time=utc_now(),
                users_json=USERS_JSON_PATH,
                output=COMPARE_OUTPUT_PATH,
                visit=VISIT_COLLECTION,
                video=VIDEO_COLLECTION,
                events=EVENTS_COLLECTION,
            )
        )


def log_line(text):
    with open(COMPARE_LOG_PATH, "a", encoding="utf-8") as log_file:
        log_file.write(text + "\n")


def load_users():
    with open(USERS_JSON_PATH, "r", encoding="utf-8") as input_file:
        return json.load(input_file)


def save_output(payload):
    with open(COMPARE_OUTPUT_PATH, "w", encoding="utf-8") as output_file:
        json.dump(payload, output_file, ensure_ascii=True, indent=2, default=str)


def parse_cutoff(value, fallback):
    dt = parse_dt(value)
    if dt is None:
        return parse_dt(fallback)
    return dt


def user_match(username, user_id):
    # if username and user_id is not None:
    #     return {"$or": [{"uName": username}, {"uId": user_id}]}
    if username:
        return {"uName": username}
    return {"uId": user_id}


def aggregate_events(collection, match_query, source):
    pipeline = [
        {"$match": match_query},
        {
            "$project": {
                "_id": 1,
                "serverTime": 1,
                "Actn": 1,
                "uName": 1,
                "uId": 1,
                "stoysId": 1,
                "sessionId": 1,
                "connectionId": 1,
            }
        },
        {"$sort": {"serverTime": 1, "_id": 1}},
    ]

    cursor = collection.aggregate(
        pipeline,
        allowDiskUse=True,
        batchSize=PAGE_SIZE,
    )

    events = []
    for doc in cursor:
        event = normalize_event(doc, source)
        if event is not None:
            events.append(event)
    return events


def find_stoys_id(existing_user_stoys_id, visit_events):
    if existing_user_stoys_id not in [None, ""]:
        return existing_user_stoys_id

    for event in visit_events:
        value = event.get("stoys_id")
        if value not in [None, ""]:
            return value
    return None


def fetch_visit_events(visit_col, username, user_id, start_at):
    match_query = {
        "$and": [
            user_match(username, user_id),
            {"serverTime": {"$gte": start_at}},
        ]
    }
    return aggregate_events(visit_col, match_query, "visit")


def fetch_video_events(video_col, username, stoys_id, start_at):
    if True or not username or stoys_id in [None, ""]:
        return []

    match_query = {
        "uName": username,
        "stoysId": stoys_id,
        "serverTime": {"$gte": start_at},
    }
    projection = {
        "_id": 1,
        "serverTime": 1,
        "Actn": 1,
        "uName": 1,
        "uId": 1,
        "stoysId": 1,
        "sessionId": 1,
        "connectionId": 1,
    }

    events = []
    cursor = (
        video_col.find(match_query, projection)
        .sort([("serverTime", 1), ("_id", 1)])
        .batch_size(PAGE_SIZE)
    )
    for doc in cursor:
        event = normalize_event(doc, "video")
        if event is not None:
            events.append(event)
    return events


def fetch_connect_disconnect_events(events_col, username, user_id, switch_at):
    match_query = {
        "$and": [
            user_match(username, user_id),
            {"serverTime": {"$gte": switch_at}},
            {"Actn": {"$in": ["Connect", "Disconnect"]}},
        ]
    }
    return aggregate_events(events_col, match_query, "events")


def calculate_user_methods(user, visit_col, video_col, events_col, thresholds, start_at, switch_at):
    username = user.get("username")
    user_id = user.get("user_id")
    log_line(f"username and id: {username} = {user_id}")
    visit_events = fetch_visit_events(visit_col, username, user_id, start_at)
    log_line(f'visit events: {len(visit_events)}')
    stoys_id = find_stoys_id(user.get("stoys_id"), visit_events)
    video_events = fetch_video_events(video_col, username, stoys_id, start_at)
    log_line(f"video events: {len(video_events)}")
    all_events = visit_events + video_events

    pre_switch_events = split_before_date(all_events, switch_at)
    connect_disconnect_events = fetch_connect_disconnect_events(events_col, username, user_id, switch_at)
    log_line(f"conn/disc events: {len(connect_disconnect_events)}")
    connect_stats = calculate_connect_disconnect(connect_disconnect_events)

    method_1 = {}
    method_2 = {}

    for threshold in thresholds:
        method1_stats = calculate_method1(all_events, threshold)
        method1_before_switch = calculate_method1(pre_switch_events, threshold)

        method_1[str(threshold)] = method1_stats
        method_2[str(threshold)] = {
            "total_seconds": method1_before_switch["total_seconds"] + connect_stats["total_seconds"],
            "pre_switch_seconds": method1_before_switch["total_seconds"],
            "post_switch_connect_seconds": connect_stats["total_seconds"],
            "pre_switch_session_count": method1_before_switch["session_count"],
            "pre_switch_event_count": method1_before_switch["event_count"],
            "connect_pair_count": connect_stats["pair_count"],
            "connect_event_count": connect_stats["event_count"],
            "connect_open_connection_count": connect_stats["open_connection_count"],
            "connect_anomaly_count": connect_stats["anomaly_count"],
        }

    result = {
        "user_id": user_id,
        "username": username,
        "stoys_id": stoys_id,
        "source_spend_time": user.get("spend_time"),
        "last_used_at": user.get('last_used_at'),
        "event_counts": {
            "visit_event_count": len(visit_events),
            "video_event_count": len(video_events),
            "all_event_count": len(all_events),
            "pre_switch_event_count": len(pre_switch_events),
            "connect_disconnect_event_count": connect_stats["event_count"],
        },
        "comparisons": {
            "method_1": method_1,
            "method_2": method_2,
        },
    }

    sample_threshold = str(thresholds[0])
    log_line(
        "time={time} user={user} user_id={user_id} stoys_id={stoys_id} events={events} method1_{thr}={m1} method2_{thr}={m2}".format(
            time=utc_now(),
            user=username,
            user_id=user_id,
            stoys_id=stoys_id,
            events=len(all_events),
            thr=sample_threshold,
            m1=method_1[sample_threshold]["total_seconds"],
            m2=method_2[sample_threshold]["total_seconds"],
        )
    )

    return result


def main():
    reset_log()
    thresholds = parse_threshold_values(THRESHOLD_VALUES)
    start_at = parse_cutoff(MIN_CREATED_AT, "2025-08-01T00:00:00Z")
    switch_at = parse_cutoff(METHOD2_SWITCH_AT, "2026-01-05T00:00:00Z")

    users = load_users()

    client = MongoClient(MONGO_URI)
    try:
        db = client[MONGO_DB]
        visit_col = db[VISIT_COLLECTION]
        video_col = db[VIDEO_COLLECTION]
        events_col = db[EVENTS_COLLECTION]

        results = []
        for user in users:
            if not user.get("username") and user.get("user_id") is None:
                continue
            result = calculate_user_methods(
                user=user,
                visit_col=visit_col,
                video_col=video_col,
                events_col=events_col,
                thresholds=thresholds,
                start_at=start_at,
                switch_at=switch_at,
            )
            results.append(result)
    finally:
        client.close()

    payload = {
        "generated_at": utc_now(),
        "source_users_json": USERS_JSON_PATH,
        "start_at": fmt_ts(start_at),
        "method2_switch_at": fmt_ts(switch_at),
        "thresholds": thresholds,
        "visit_collection": VISIT_COLLECTION,
        "video_collection": VIDEO_COLLECTION,
        "events_collection": EVENTS_COLLECTION,
        "users": results,
    }
    save_output(payload)

    summary = "time={time} stage=done users={count} output={output}".format(
        time=utc_now(),
        count=len(results),
        output=COMPARE_OUTPUT_PATH,
    )
    log_line(summary)
    print(summary)


if __name__ == "__main__":
    main()
