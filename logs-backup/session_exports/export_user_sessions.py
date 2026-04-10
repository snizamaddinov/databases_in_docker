import argparse
import csv
import json
import re
import sys
from datetime import datetime, timezone
from pathlib import Path

from pymongo import MongoClient

ROOT_DIR = Path(__file__).resolve().parents[1]
if str(ROOT_DIR) not in sys.path:
    sys.path.insert(0, str(ROOT_DIR))

from config import METHOD2_SWITCH_AT, MONGO_DB, MONGO_URI, PAGE_SIZE, VISIT_COLLECTION  # noqa: E402
from spend_time_methods_core import fmt_ts, normalize_event, parse_dt  # noqa: E402

DIMENSION_PREFIXES = {
    "brand": "brand",
    "branch": "branch",
    "userType": "user_type",
    "schName": "school_name",
    "grade": "grade",
    "class": "class_name",
    "config_browser": "browser",
    "config_os_version": "os_version",
    "config_os": "os",
    "config_device_type": "device_type",
    "config_device_model": "device_model",
    "country": "country",
    "city": "city",
    "ip": "ip",
    "userAgent": "user_agent",
    "path": "path_from_bucket",
}
DIMENSION_PREFIX_ORDER = sorted(DIMENSION_PREFIXES.keys(), key=len, reverse=True)


def utc_now():
    return datetime.now(timezone.utc).isoformat().replace("+00:00", "Z")


def parse_cutoff(value, fallback):
    parsed_value = parse_dt(value)
    if parsed_value is not None:
        return parsed_value
    return parse_dt(fallback)


def user_match(username, user_id):
    if username:
        return {"uName": username}
    return {"uId": user_id}


def extract_dimension_details(dimension_bucket):
    details = {field_name: "" for field_name in DIMENSION_PREFIXES.values()}
    if not isinstance(dimension_bucket, list):
        return details

    for item in dimension_bucket:
        if not isinstance(item, str):
            continue
        for prefix in DIMENSION_PREFIX_ORDER:
            token = f"{prefix}-"
            if item.startswith(token):
                details[DIMENSION_PREFIXES[prefix]] = item[len(token) :]
                break

    return details


def normalize_visit_event(doc):
    event = normalize_event(doc, "visit")
    if event is None:
        return None

    details = extract_dimension_details(doc.get("dimensionBucket"))
    event.update(details)

    raw_path = doc.get("path")
    if raw_path in [None, ""]:
        raw_path = details.get("path_from_bucket", "")
    event["path"] = raw_path

    return event


def fetch_connect_disconnect_events(visit_col, username, user_id, switch_at):
    match_query = {
        "$and": [
            user_match(username, user_id),
            {"serverTime": {"$gte": switch_at}},
            {"Actn": {"$in": ["Connect", "Disconnect"]}},
        ]
    }

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
                "path": 1,
                "dimensionBucket": 1,
            }
        },
        {"$sort": {"serverTime": 1, "_id": 1}},
    ]

    events = []
    cursor = visit_col.aggregate(pipeline, allowDiskUse=True, batchSize=PAGE_SIZE)
    for doc in cursor:
        event = normalize_visit_event(doc)
        if event is not None:
            events.append(event)
    return events


def build_sessions_by_session_id(events):
    grouped = {}

    for event in sorted(events, key=lambda item: (item["time"], item["event_id"])):
        session_id = event.get("session_id")
        if not session_id:
            continue

        group = grouped.setdefault(
            session_id,
            {
                "session_id": session_id,
                "first_connect_event": None,
                "last_disconnect_event": None,
                "event_count": 0,
                "connect_event_count": 0,
                "disconnect_event_count": 0,
            },
        )
        group["event_count"] += 1

        action = event.get("action")
        if action == "Connect":
            group["connect_event_count"] += 1
            if group["first_connect_event"] is None:
                group["first_connect_event"] = event
            continue

        if action == "Disconnect":
            group["disconnect_event_count"] += 1
            group["last_disconnect_event"] = event

    sessions = []
    missing_connect_count = 0
    missing_disconnect_count = 0
    negative_duration_count = 0

    for group in grouped.values():
        first_connect_event = group["first_connect_event"]
        last_disconnect_event = group["last_disconnect_event"]

        if first_connect_event is None:
            missing_connect_count += 1
            continue
        if last_disconnect_event is None:
            missing_disconnect_count += 1
            continue

        start_time = first_connect_event["time"]
        end_time = last_disconnect_event["time"]
        duration_seconds = int((end_time - start_time).total_seconds())
        if duration_seconds < 0:
            negative_duration_count += 1
            continue

        sessions.append(
            {
                "session_id": group["session_id"],
                "start_time": start_time,
                "end_time": end_time,
                "duration_seconds": duration_seconds,
                "event_count": group["event_count"],
                "connect_event_count": group["connect_event_count"],
                "disconnect_event_count": group["disconnect_event_count"],
                "start_event": first_connect_event,
                "end_event": last_disconnect_event,
            }
        )

    sessions.sort(key=lambda item: (item["start_time"], item["end_time"], item["session_id"]))

    return {
        "session_id_count": len(grouped),
        "events_with_session_id_count": sum(item["event_count"] for item in grouped.values()),
        "valid_session_count": len(sessions),
        "missing_connect_count": missing_connect_count,
        "missing_disconnect_count": missing_disconnect_count,
        "negative_duration_count": negative_duration_count,
        "sessions": sessions,
    }


def first_non_empty(*values):
    for value in values:
        if value not in [None, ""]:
            return value
    return ""


def build_row(session, session_order):
    start_event = session["start_event"]
    end_event = session["end_event"]

    return {
        "date": session["start_time"].date().isoformat(),
        "session_start_time": fmt_ts(session["start_time"]),
        "session_end_time": fmt_ts(session["end_time"]),
        "duration_seconds": session["duration_seconds"],
        "session_order": session_order,
        "algorithm_segment": "session_id_first_connect_last_disconnect",
        "session_id": session["session_id"],
        "event_count": session["event_count"],
        "connect_event_count": session["connect_event_count"],
        "disconnect_event_count": session["disconnect_event_count"],
        "start_event_id": start_event.get("event_id", ""),
        "end_event_id": end_event.get("event_id", ""),
        "start_action": start_event.get("action", ""),
        "end_action": end_event.get("action", ""),
        "username": first_non_empty(start_event.get("u_name"), end_event.get("u_name")),
        "user_id": first_non_empty(start_event.get("u_id"), end_event.get("u_id")),
        "stoys_id": first_non_empty(start_event.get("stoys_id"), end_event.get("stoys_id")),
        "path": first_non_empty(start_event.get("path"), end_event.get("path")),
        "browser": first_non_empty(start_event.get("browser"), end_event.get("browser")),
        "os": first_non_empty(start_event.get("os"), end_event.get("os")),
        "os_version": first_non_empty(start_event.get("os_version"), end_event.get("os_version")),
        "device_type": first_non_empty(start_event.get("device_type"), end_event.get("device_type")),
        "device_model": first_non_empty(start_event.get("device_model"), end_event.get("device_model")),
        "country": first_non_empty(start_event.get("country"), end_event.get("country")),
        "city": first_non_empty(start_event.get("city"), end_event.get("city")),
        "ip": first_non_empty(start_event.get("ip"), end_event.get("ip")),
        "user_agent": first_non_empty(start_event.get("user_agent"), end_event.get("user_agent")),
        "brand": first_non_empty(start_event.get("brand"), end_event.get("brand")),
        "branch": first_non_empty(start_event.get("branch"), end_event.get("branch")),
        "user_type": first_non_empty(start_event.get("user_type"), end_event.get("user_type")),
        "school_name": first_non_empty(start_event.get("school_name"), end_event.get("school_name")),
        "grade": first_non_empty(start_event.get("grade"), end_event.get("grade")),
        "class_name": first_non_empty(start_event.get("class_name"), end_event.get("class_name")),
    }


def group_rows_by_date(rows):
    grouped = {}
    for row in rows:
        grouped.setdefault(row["date"], []).append(row)
    return {date: grouped[date] for date in sorted(grouped.keys())}


def slugify(value):
    cleaned = re.sub(r"[^A-Za-z0-9._-]+", "_", str(value).strip())
    return cleaned.strip("._") or "user_sessions"


def parse_args():
    parser = argparse.ArgumentParser(
        description="Export one session per sessionId using first Connect and last Disconnect after METHOD2_SWITCH_AT."
    )
    parser.add_argument("--username", default="", help="Username to query (preferred if both username and user-id exist).")
    parser.add_argument("--user-id", type=int, default=None, help="User ID to query when username is not provided.")
    parser.add_argument("--switch-at", default=METHOD2_SWITCH_AT, help="Lower bound for serverTime (ISO-8601).")
    parser.add_argument(
        "--output-dir",
        default=str(ROOT_DIR / "session_exports" / "data"),
        help="Output directory for CSV/JSON exports.",
    )
    parser.add_argument(
        "--output-prefix",
        default="",
        help="Optional output file prefix. Defaults to username or user_id.",
    )
    return parser.parse_args()


def main():
    args = parse_args()
    if not args.username and args.user_id is None:
        raise SystemExit("Provide --username or --user-id.")

    switch_at = parse_cutoff(args.switch_at, METHOD2_SWITCH_AT)
    if switch_at is None:
        raise SystemExit(f"Invalid --switch-at value: {args.switch_at}")

    client = MongoClient(MONGO_URI)
    try:
        db = client[MONGO_DB]
        visit_col = db[VISIT_COLLECTION]
        connect_disconnect_events = fetch_connect_disconnect_events(
            visit_col=visit_col,
            username=args.username,
            user_id=args.user_id,
            switch_at=switch_at,
        )
    finally:
        client.close()

    session_stats = build_sessions_by_session_id(connect_disconnect_events)
    session_rows = [build_row(session, idx) for idx, session in enumerate(session_stats["sessions"], start=1)]

    output_dir = Path(args.output_dir)
    output_dir.mkdir(parents=True, exist_ok=True)

    default_prefix = args.username if args.username else f"user_{args.user_id}"
    prefix = slugify(args.output_prefix or default_prefix)
    csv_path = output_dir / f"{prefix}_sessions.csv"
    json_path = output_dir / f"{prefix}_sessions.json"

    fieldnames = [
        "date",
        "session_start_time",
        "session_end_time",
        "duration_seconds",
        "session_order",
        "algorithm_segment",
        "session_id",
        "event_count",
        "connect_event_count",
        "disconnect_event_count",
        "start_event_id",
        "end_event_id",
        "start_action",
        "end_action",
        "username",
        "user_id",
        "stoys_id",
        "path",
        "browser",
        "os",
        "os_version",
        "device_type",
        "device_model",
        "country",
        "city",
        "ip",
        "user_agent",
        "brand",
        "branch",
        "user_type",
        "school_name",
        "grade",
        "class_name",
    ]

    with open(csv_path, "w", encoding="utf-8", newline="") as csv_file:
        writer = csv.DictWriter(csv_file, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(session_rows)

    total_seconds = sum(row["duration_seconds"] for row in session_rows)
    payload = {
        "generated_at": utc_now(),
        "query": {
            "username": args.username,
            "user_id": args.user_id,
            "switch_at": fmt_ts(switch_at),
            "visit_collection": VISIT_COLLECTION,
        },
        "event_counts": {
            "connect_disconnect_event_count": len(connect_disconnect_events),
            "events_with_session_id_count": session_stats["events_with_session_id_count"],
            "session_id_group_count": session_stats["session_id_count"],
        },
        "totals": {
            "session_count": len(session_rows),
            "total_seconds": total_seconds,
            "missing_connect_count": session_stats["missing_connect_count"],
            "missing_disconnect_count": session_stats["missing_disconnect_count"],
            "negative_duration_count": session_stats["negative_duration_count"],
        },
        "sessions_by_date": group_rows_by_date(session_rows),
    }

    with open(json_path, "w", encoding="utf-8") as json_file:
        json.dump(payload, json_file, ensure_ascii=True, indent=2, default=str)

    print(f"Exported {len(session_rows)} sessions")
    print(f"Total seconds: {total_seconds}")
    print(f"CSV: {csv_path}")
    print(f"JSON: {json_path}")


if __name__ == "__main__":
    main()
