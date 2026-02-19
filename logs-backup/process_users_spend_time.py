import json
from datetime import datetime, timezone

from pymongo import MongoClient

from config import (
    MIN_CREATED_AT,
    MONGO_DB,
    MONGO_URI,
    PAGE_SIZE,
    PROCESS_LOG_PATH,
    PROCESS_ONLY_NEW,
    THRESHOLD_SECONDS,
    USERS_JSON_PATH,
    VIDEO_COLLECTION,
    VISIT_COLLECTION,
)


VIDEO_ACTIONS = {
    "VideoWatched",
    "VideoPlayed",
    "VideoPaused",
    "VideoResumed",
    "VideoWatching",
    "VideoCompleted",
}


def utc_now():
    return datetime.now(timezone.utc).isoformat().replace("+00:00", "Z")


def parse_dt(value):
    if value is None:
        return None

    if isinstance(value, datetime):
        if value.tzinfo is None:
            return value.replace(tzinfo=timezone.utc)
        return value.astimezone(timezone.utc)

    if isinstance(value, str):
        txt = value.strip()
        if txt.endswith("Z"):
            txt = txt[:-1] + "+00:00"
        try:
            dt = datetime.fromisoformat(txt)
        except ValueError:
            return None
        if dt.tzinfo is None:
            return dt.replace(tzinfo=timezone.utc)
        return dt.astimezone(timezone.utc)

    return None


def parse_cutoff():
    dt = parse_dt(MIN_CREATED_AT)
    if dt is None:
        return datetime(2025, 8, 1, tzinfo=timezone.utc)
    return dt


def fmt_ts(dt):
    if dt is None:
        return "none"
    return dt.isoformat().replace("+00:00", "Z")


def reset_log():
    with open(PROCESS_LOG_PATH, "w", encoding="utf-8") as log_file:
        log_file.write(
            f"time={utc_now()} stage=start cutoff={fmt_ts(CUTOFF_AT)} threshold={THRESHOLD_SECONDS}\n"
        )


def log_line(text):
    with open(PROCESS_LOG_PATH, "a", encoding="utf-8") as log_file:
        log_file.write(text + "\n")


def add_id_pagination(query, last_id):
    if last_id is None:
        return query
    if "_id" in query:
        return {"$and": [query, {"_id": {"$gt": last_id}}]}
    copy_query = dict(query)
    copy_query["_id"] = {"$gt": last_id}
    return copy_query


def paginated_find(collection, base_query, projection):
    last_id = None
    while True:
        query = add_id_pagination(base_query, last_id)
        docs = list(collection.find(query, projection).sort("_id", 1).limit(PAGE_SIZE))
        if not docs:
            break
        for doc in docs:
            yield doc
        last_id = docs[-1].get("_id")
        if last_id is None:
            break


def normalize_event(doc, source):
    event_time = parse_dt(doc.get("serverTime"))
    if event_time is None:
        event_time = parse_dt(doc.get("createdAt"))
    if event_time is None:
        return None

    return {
        "event_id": str(doc.get("_id")),
        "time": event_time,
        "created_at": parse_dt(doc.get("createdAt")),
        "source": source,
        "action": doc.get("Actn") or "",
        "u_name": doc.get("uName"),
        "u_id": doc.get("uId"),
        "stoys_id": doc.get("stoysId"),
    }


def visit_query(username, user_id):
    created_filter = {"createdAt": {"$gt": CUTOFF_AT}}
    if user_id is not None and username:
        return {"$and": [created_filter, {"$or": [{"uId": user_id}, {"uName": username}]}]}
    if user_id is not None:
        return {"$and": [created_filter, {"uId": user_id}]}
    if username:
        return {"$and": [created_filter, {"uName": username}]}
    return created_filter


def fetch_visit_events(visit_col, username, user_id):
    projection = {
        "uName": 1,
        "uId": 1,
        "stoysId": 1,
        "Actn": 1,
        "serverTime": 1,
        "createdAt": 1,
    }

    events = []
    stoys_id = None
    for doc in paginated_find(visit_col, visit_query(username, user_id), projection):
        event = normalize_event(doc, "visit")
        if event is None:
            continue
        events.append(event)

        event_stoys = event.get("stoys_id")
        if event_stoys is None or str(event_stoys).strip() == "":
            continue

        if stoys_id is None:
            stoys_id = event_stoys
        elif str(stoys_id) != str(event_stoys):
            log_line(
                f"time={utc_now()} user={username} user_id={user_id} stoys_id_mismatch keep={stoys_id} skip={event_stoys}"
            )
    return events, stoys_id


def fetch_video_events(video_col, username, stoys_id):
    if not username or not stoys_id:
        return []

    projection = {
        "uName": 1,
        "uId": 1,
        "stoysId": 1,
        "Actn": 1,
        "serverTime": 1,
        "createdAt": 1,
    }
    query = {
        "createdAt": {"$gt": CUTOFF_AT},
        "uName": username,
        "stoysId": stoys_id,
    }

    events = []
    for doc in paginated_find(video_col, query, projection):
        event = normalize_event(doc, "video")
        if event is not None:
            events.append(event)
    return events


def calculate_spend_time(username, user_id, events):
    if not events:
        return 0, 0

    events.sort(key=lambda item: (item["time"], item["event_id"]))
    session_start = events[0]["time"]
    last_time = events[0]["time"]
    session_count = 1
    total_spent_seconds = 0

    first_event = events[0]
    log_line(
        "time={time} user={user} user_id={user_id} event={event} source={source} event_time={event_time} gap=0 decision=start-session".format(
            time=utc_now(),
            user=username,
            user_id=user_id,
            event=first_event.get("action"),
            source=first_event.get("source"),
            event_time=fmt_ts(first_event.get("time")),
        )
    )

    for event in events[1:]:
        current_time = event["time"]
        gap = (current_time - last_time).total_seconds()
        if gap < 0:
            gap = 0

        decision = "add"
        reason = "within-threshold"

        if gap > THRESHOLD_SECONDS:
            is_video_event = event.get("source") == "video" or event.get("action") in VIDEO_ACTIONS
            if is_video_event:
                decision = "add"
                reason = "video-gap"
            else:
                duration = int((last_time - session_start).total_seconds())
                if duration > 0:
                    total_spent_seconds += duration
                session_start = current_time
                session_count += 1
                decision = "new-session"
                reason = "gap-over-threshold"

        last_time = current_time
        log_line(
            "time={time} user={user} user_id={user_id} event={event} source={source} event_time={event_time} gap={gap} decision={decision} reason={reason}".format(
                time=utc_now(),
                user=username,
                user_id=user_id,
                event=event.get("action"),
                source=event.get("source"),
                event_time=fmt_ts(current_time),
                gap=int(gap),
                decision=decision,
                reason=reason,
            )
        )

    final_duration = int((last_time - session_start).total_seconds())
    if final_duration > 0:
        total_spent_seconds += final_duration

    return total_spent_seconds, session_count


def process_user(user, visit_col, video_col):
    username = user.get("username")
    user_id = user.get("user_id")

    visit_events, stoys_id = fetch_visit_events(visit_col, username, user_id)
    if not stoys_id and user.get("stoys_id"):
        stoys_id = user.get("stoys_id")

    video_events = fetch_video_events(video_col, username, stoys_id)

    all_events = visit_events + video_events
    new_spend_time_seconds, session_count = calculate_spend_time(username, user_id, all_events)

    user["stoys_id"] = stoys_id
    user["new_spend_time_seconds"] = new_spend_time_seconds
    user["status"] = "processed"
    user["calc_meta"] = {
        "visit_event_count": len(visit_events),
        "video_event_count": len(video_events),
        "session_count": session_count,
        "threshold_seconds": THRESHOLD_SECONDS,
        "cutoff_created_at": fmt_ts(CUTOFF_AT),
    }

    log_line(
        "time={time} user={user} user_id={user_id} stoys_id={stoys_id} visit_events={visit_count} video_events={video_count} sessions={sessions} new_spend_time_seconds={spent}".format(
            time=utc_now(),
            user=username,
            user_id=user_id,
            stoys_id=stoys_id,
            visit_count=len(visit_events),
            video_count=len(video_events),
            sessions=session_count,
            spent=new_spend_time_seconds,
        )
    )


def main():
    reset_log()

    with open(USERS_JSON_PATH, "r", encoding="utf-8") as input_file:
        users = json.load(input_file)

    client = MongoClient(MONGO_URI)
    try:
        db = client[MONGO_DB]
        visit_col = db[VISIT_COLLECTION]
        video_col = db[VIDEO_COLLECTION]

        processed = 0
        skipped = 0
        for user in users:
            if PROCESS_ONLY_NEW and user.get("status") != "new":
                skipped += 1
                continue
            process_user(user, visit_col, video_col)
            processed += 1

        with open(USERS_JSON_PATH, "w", encoding="utf-8") as output_file:
            json.dump(users, output_file, ensure_ascii=True, indent=2, default=str)
    finally:
        client.close()

    summary = f"time={utc_now()} stage=done processed={processed} skipped={skipped} output={USERS_JSON_PATH}"
    log_line(summary)
    print(summary)


CUTOFF_AT = parse_cutoff()


if __name__ == "__main__":
    main()
