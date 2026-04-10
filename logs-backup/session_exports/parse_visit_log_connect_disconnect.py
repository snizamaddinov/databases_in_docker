import argparse
import csv
import json
from collections import deque
from datetime import datetime, timezone
from pathlib import Path

DIMENSION_PREFIXES = {
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


FIELDNAMES = [
    "date",
    "connectedAt",
    "disconnectedAt",
    "spentTimeSeconds",
    "connectionId",
    "sessionId",
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
]


def parse_dt(value):
    if isinstance(value, dict) and "$date" in value:
        value = value["$date"]

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


def fmt_ts(dt):
    return dt.isoformat(timespec="milliseconds")


def extract_dimension_values(bucket):
    values = {field_name: "" for field_name in DIMENSION_PREFIXES.values()}
    if not isinstance(bucket, list):
        return values

    for item in bucket:
        if not isinstance(item, str):
            continue
        for prefix in DIMENSION_PREFIX_ORDER:
            token = f"{prefix}-"
            if item.startswith(token):
                values[DIMENSION_PREFIXES[prefix]] = item[len(token) :]
                break

    return values


def load_documents(payload):
    if isinstance(payload, list):
        return payload
    if isinstance(payload, dict):
        for key in ("documents", "data", "items", "rows"):
            if isinstance(payload.get(key), list):
                return payload[key]
    raise ValueError("Unsupported JSON structure. Expected a list of log documents.")


def build_event_row(doc):
    action = doc.get("Actn")
    if action not in {"Connect", "Disconnect"}:
        return None

    event_time = parse_dt(doc.get("serverTime"))
    if event_time is None:
        return None

    dim = extract_dimension_values(doc.get("dimensionBucket"))
    path_value = doc.get("path")
    if path_value in [None, ""]:
        path_value = dim.get("path_from_bucket", "")

    return {
        "_sort_key": (event_time, str(doc.get("_id", ""))),
        "_event_time": event_time,
        "_action": action,
        "date": event_time.date().isoformat(),
        "connectedAt": fmt_ts(event_time) if action == "Connect" else "",
        "disconnectedAt": fmt_ts(event_time) if action == "Disconnect" else "",
        "spentTimeSeconds": "",
        "connectionId": doc.get("connectionId") or "",
        "sessionId": doc.get("sessionId") or "",
        "path": path_value or "",
        "browser": dim.get("browser", ""),
        "os": dim.get("os", ""),
        "os_version": dim.get("os_version", ""),
        "device_type": dim.get("device_type", ""),
        "device_model": dim.get("device_model", ""),
        "country": dim.get("country", ""),
        "city": dim.get("city", ""),
        "ip": dim.get("ip", ""),
        "user_agent": dim.get("user_agent", ""),
    }


def parse_args():
    parser = argparse.ArgumentParser(
        description="List Connect/Disconnect requests from a visit-log JSON file in time order."
    )
    parser.add_argument(
        "--input",
        default="session_exports/matomo.visit_log_2526.json",
        help="Path to the input JSON file.",
    )
    parser.add_argument(
        "--output",
        default="session_exports/data/connect_disconnect_requests.csv",
        help="Path to the output CSV file.",
    )
    return parser.parse_args()


def main():
    args = parse_args()

    input_path = Path(args.input)
    output_path = Path(args.output)
    output_path.parent.mkdir(parents=True, exist_ok=True)

    with open(input_path, "r", encoding="utf-8") as input_file:
        payload = json.load(input_file)

    documents = load_documents(payload)

    rows = []
    for doc in documents:
        row = build_event_row(doc)
        if row is not None:
            rows.append(row)

    rows.sort(key=lambda item: item["_sort_key"])

    open_connects = {}
    for row in rows:
        key = (row["sessionId"], row["connectionId"])
        if row["_action"] == "Connect":
            queue = open_connects.setdefault(key, deque())
            queue.append(row["_event_time"])
            continue

        queue = open_connects.get(key)
        if not queue:
            continue
        start_time = queue.popleft()
        duration_seconds = (row["_event_time"] - start_time).total_seconds()
        if duration_seconds < 0:
            continue
        row["spentTimeSeconds"] = f"{duration_seconds:.3f}"

    for row in rows:
        row.pop("_sort_key", None)
        row.pop("_event_time", None)
        row.pop("_action", None)

    with open(output_path, "w", encoding="utf-8", newline="") as output_file:
        writer = csv.DictWriter(output_file, fieldnames=FIELDNAMES)
        writer.writeheader()
        writer.writerows(rows)

    print(f"Parsed events: {len(rows)}")
    print(f"Output CSV: {output_path}")


if __name__ == "__main__":
    main()
