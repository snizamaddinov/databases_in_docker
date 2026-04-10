import argparse
import csv
import json
import os
import re
import sys
from datetime import datetime, timezone
from pathlib import Path

ROOT_DIR = Path(__file__).resolve().parents[1]
SCRIPT_DIR = Path(__file__).resolve().parent
if str(ROOT_DIR) not in sys.path:
    sys.path.insert(0, str(ROOT_DIR))

from spend_time_methods_core import (  # noqa: E402
    calculate_connect_disconnect_with_sessions,
    calculate_method1_with_sessions,
    fmt_ts,
    normalize_event,
    parse_dt,
    split_before_date,
)

DEFAULT_METHOD2_SWITCH_AT = os.getenv("METHOD2_SWITCH_AT", "2026-01-05T00:00:00Z")

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

CSV_FIELDNAMES = [
    "date",
    "session_start_time",
    "session_end_time",
    "duration_seconds",
    "counted_in_total",
    "session_order",
    "segment_session_order",
    "algorithm",
    "algorithm_segment",
    "threshold_seconds",
    "method2_switch_at",
    "event_count",
    "start_event_id",
    "end_event_id",
    "start_action",
    "end_action",
    "session_id",
    "connection_id",
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


def utc_now():
    return datetime.now(timezone.utc).isoformat().replace("+00:00", "Z")


def parse_cutoff(value, fallback):
    parsed = parse_dt(value)
    if parsed is None:
        parsed = parse_dt(fallback)
    return parsed


def slugify(value):
    cleaned = re.sub(r"[^A-Za-z0-9._-]+", "_", str(value).strip())
    return cleaned.strip("._") or "user_sessions"


def unwrap_mongo_date(value):
    if isinstance(value, dict) and "$date" in value:
        return value.get("$date")
    return value


def unwrap_mongo_id(value):
    if isinstance(value, dict) and "$oid" in value:
        return value.get("$oid")
    return value


def load_documents(payload):
    if isinstance(payload, list):
        return payload
    if isinstance(payload, dict):
        for key in ("documents", "data", "items", "rows"):
            if isinstance(payload.get(key), list):
                return payload[key]
    raise ValueError("Unsupported JSON structure. Expected a list of log documents.")


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
    prepared = {
        "_id": unwrap_mongo_id(doc.get("_id")),
        "serverTime": unwrap_mongo_date(doc.get("serverTime")),
        "Actn": doc.get("Actn"),
        "uName": doc.get("uName"),
        "uId": doc.get("uId"),
        "stoysId": doc.get("stoysId"),
        "sessionId": doc.get("sessionId"),
        "connectionId": doc.get("connectionId"),
    }
    event = normalize_event(prepared, "visit")
    if event is None:
        return None

    details = extract_dimension_details(doc.get("dimensionBucket"))
    event.update(details)

    raw_path = doc.get("path")
    if raw_path in [None, ""]:
        raw_path = details.get("path_from_bucket", "")
    event["path"] = raw_path or ""

    return event


def first_non_empty(*values):
    for value in values:
        if value not in [None, ""]:
            return value
    return ""


def resolve_target_user(events, username, user_id):
    if username:
        return username, user_id

    if user_id is not None:
        detected_username = ""
        for event in events:
            if event.get("u_id") == user_id and event.get("u_name") not in [None, ""]:
                detected_username = event.get("u_name")
                break
        return detected_username, user_id

    usernames = sorted({event.get("u_name") for event in events if event.get("u_name") not in [None, ""]})
    if len(usernames) == 1:
        username_value = usernames[0]
        ids_for_username = sorted(
            {event.get("u_id") for event in events if event.get("u_name") == username_value and event.get("u_id") is not None}
        )
        user_id_value = ids_for_username[0] if len(ids_for_username) == 1 else None
        return username_value, user_id_value

    raise SystemExit("Multiple usernames found. Provide --username or --user-id.")


def filter_events_by_user(events, username, user_id):
    if username:
        return [event for event in events if event.get("u_name") == username]
    return [event for event in events if event.get("u_id") == user_id]


def build_row(session, algorithm_segment, threshold_seconds, switch_at):
    start_event = session["start_event"]
    end_event = session["end_event"]
    segment_session_order = first_non_empty(session.get("session_index"), session.get("pair_index"))
    counted_in_total = session.get("counted_in_total", True)

    return {
        "date": session["start_time"].date().isoformat(),
        "session_start_time": fmt_ts(session["start_time"]),
        "session_end_time": fmt_ts(session["end_time"]),
        "duration_seconds": session["duration_seconds"],
        "counted_in_total": counted_in_total,
        "session_order": "",
        "segment_session_order": segment_session_order,
        "algorithm": "method_2",
        "algorithm_segment": algorithm_segment,
        "threshold_seconds": threshold_seconds if algorithm_segment == "pre_switch_method1_threshold" else "",
        "method2_switch_at": fmt_ts(switch_at),
        "event_count": session.get("event_count", ""),
        "start_event_id": start_event.get("event_id", ""),
        "end_event_id": end_event.get("event_id", ""),
        "start_action": start_event.get("action", ""),
        "end_action": end_event.get("action", ""),
        "session_id": first_non_empty(session.get("session_id"), start_event.get("session_id"), end_event.get("session_id")),
        "connection_id": first_non_empty(
            session.get("connection_id"), start_event.get("connection_id"), end_event.get("connection_id")
        ),
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
        "_sort_start": session["start_time"],
        "_sort_end": session["end_time"],
    }


def group_rows_by_date(rows):
    grouped = {}
    for row in rows:
        grouped.setdefault(row["date"], []).append(row)
    for date_value in grouped:
        grouped[date_value].sort(key=lambda item: (item["session_start_time"], item["session_end_time"]))
    return {date_value: grouped[date_value] for date_value in sorted(grouped.keys())}


def parse_args():
    parser = argparse.ArgumentParser(
        description="Export per-session rows from a local visit-log JSON using existing method split logic."
    )
    parser.add_argument(
        "--input",
        default=str(SCRIPT_DIR / "data" / "matomo.visit_log_2526.json"),
        help="Path to exported visit-log JSON file.",
    )
    parser.add_argument("--username", default="", help="Username to export. Auto-detected when omitted.")
    parser.add_argument("--user-id", type=int, default=None, help="User ID to export.")
    parser.add_argument("--switch-at", default=DEFAULT_METHOD2_SWITCH_AT, help="Method2 switch point (ISO-8601 UTC).")
    parser.add_argument(
        "--threshold-seconds",
        type=int,
        default=300,
        help="Session gap threshold for pre-switch method1 sessions.",
    )
    parser.add_argument(
        "--output-dir",
        default=str(SCRIPT_DIR / "data"),
        help="Output directory for CSV/JSON files.",
    )
    parser.add_argument(
        "--output-prefix",
        default="",
        help="Optional output filename prefix. Defaults to username or user-id.",
    )
    return parser.parse_args()


def main():
    args = parse_args()

    if args.threshold_seconds <= 0:
        raise SystemExit("--threshold-seconds must be a positive integer.")

    switch_at = parse_cutoff(args.switch_at, DEFAULT_METHOD2_SWITCH_AT)
    if switch_at is None:
        raise SystemExit(f"Invalid --switch-at value: {args.switch_at}")

    input_path = Path(args.input)
    output_dir = Path(args.output_dir)
    output_dir.mkdir(parents=True, exist_ok=True)

    with open(input_path, "r", encoding="utf-8") as input_file:
        payload = json.load(input_file)

    docs = load_documents(payload)
    events = []
    for doc in docs:
        event = normalize_visit_event(doc)
        if event is not None:
            events.append(event)

    if not events:
        raise SystemExit("No valid events found in input file.")

    target_username, target_user_id = resolve_target_user(events, args.username, args.user_id)
    user_events = filter_events_by_user(events, target_username, target_user_id)
    if not user_events:
        raise SystemExit("No events found for requested user.")

    pre_switch_events = split_before_date(user_events, switch_at)
    connect_disconnect_events = [
        event
        for event in user_events
        if event["time"] >= switch_at and event.get("action") in {"Connect", "Disconnect"}
    ]

    method1_all = calculate_method1_with_sessions(user_events, args.threshold_seconds)
    method1_pre_switch = calculate_method1_with_sessions(pre_switch_events, args.threshold_seconds)
    connect_stats = calculate_connect_disconnect_with_sessions(connect_disconnect_events)

    rows = []
    for session in method1_pre_switch["sessions"]:
        rows.append(build_row(session, "pre_switch_method1_threshold", args.threshold_seconds, switch_at))
    for session in connect_stats["sessions"]:
        rows.append(build_row(session, "post_switch_connect_disconnect", args.threshold_seconds, switch_at))

    rows.sort(key=lambda item: (item["_sort_start"], item["_sort_end"], item["start_event_id"]))
    for idx, row in enumerate(rows, start=1):
        row["session_order"] = idx
        row.pop("_sort_start", None)
        row.pop("_sort_end", None)

    default_prefix = target_username if target_username else f"user_{target_user_id}"
    prefix = slugify(args.output_prefix or default_prefix)
    csv_path = output_dir / f"{prefix}_daily_sessions.csv"
    json_path = output_dir / f"{prefix}_daily_sessions.json"

    with open(csv_path, "w", encoding="utf-8", newline="") as csv_file:
        writer = csv.DictWriter(csv_file, fieldnames=CSV_FIELDNAMES)
        writer.writeheader()
        writer.writerows(rows)

    method2_total_seconds = method1_pre_switch["total_seconds"] + connect_stats["total_seconds"]
    row_counted_total_seconds = sum(
        int(row["duration_seconds"]) for row in rows if bool(row.get("counted_in_total"))
    )

    output_payload = {
        "generated_at": utc_now(),
        "input_file": str(input_path),
        "query": {
            "username": target_username,
            "user_id": target_user_id,
            "switch_at": fmt_ts(switch_at),
            "threshold_seconds": args.threshold_seconds,
        },
        "event_counts": {
            "input_document_count": len(docs),
            "normalized_event_count": len(events),
            "user_event_count": len(user_events),
            "pre_switch_event_count": len(pre_switch_events),
            "post_switch_connect_disconnect_event_count": len(connect_disconnect_events),
        },
        "totals": {
            "method1_all_seconds": method1_all["total_seconds"],
            "method1_all_session_count": method1_all["session_count"],
            "method2_pre_switch_method1_seconds": method1_pre_switch["total_seconds"],
            "method2_pre_switch_session_count": method1_pre_switch["session_count"],
            "method2_post_switch_connect_seconds": connect_stats["total_seconds"],
            "method2_post_switch_pair_count": connect_stats["pair_count"],
            "method2_connect_open_connection_count": connect_stats["open_connection_count"],
            "method2_connect_anomaly_count": connect_stats["anomaly_count"],
            "method2_total_seconds": method2_total_seconds,
            "rows_session_count": len(rows),
            "rows_counted_total_seconds": row_counted_total_seconds,
            "rows_total_matches_method2": row_counted_total_seconds == method2_total_seconds,
        },
        "sessions_by_date": group_rows_by_date(rows),
    }

    with open(json_path, "w", encoding="utf-8") as json_file:
        json.dump(output_payload, json_file, ensure_ascii=True, indent=2, default=str)

    print(f"User: username={target_username} user_id={target_user_id}")
    print(f"Rows exported: {len(rows)}")
    print(f"Method2 total seconds: {method2_total_seconds}")
    print(f"CSV: {csv_path}")
    print(f"JSON: {json_path}")


if __name__ == "__main__":
    main()
