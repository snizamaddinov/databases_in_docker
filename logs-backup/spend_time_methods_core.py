from datetime import datetime, timezone


SESSION_BREAK_ACTIONS = {
    "Connect",
    "LoggedIn",
    "LoggedInWithQRCode",
}


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


def fmt_ts(dt):
    if dt is None:
        return "none"
    return dt.isoformat().replace("+00:00", "Z")


def parse_threshold_values(raw):
    values = []
    for item in str(raw).split(","):
        token = item.strip()
        if not token:
            continue
        try:
            num = int(token)
        except ValueError:
            continue
        if num > 0:
            values.append(num)
    if not values:
        return [300]
    return sorted(set(values))


def normalize_event(doc, source):
    event_time = parse_dt(doc.get("serverTime"))
    if event_time is None:
        return None

    return {
        "event_id": str(doc.get("_id")),
        "time": event_time,
        "source": source,
        "action": doc.get("Actn") or "",
        "u_name": doc.get("uName"),
        "u_id": doc.get("uId"),
        "stoys_id": doc.get("stoysId"),
        "session_id": doc.get("sessionId"),
        "connection_id": doc.get("connectionId"),
    }


def calculate_method1(events, threshold_seconds):
    if not events:
        return {
            "total_seconds": 0,
            "session_count": 0,
            "event_count": 0,
        }

    sorted_events = sorted(events, key=lambda item: (item["time"], item["event_id"]))
    session_start = sorted_events[0]["time"]
    last_time = sorted_events[0]["time"]
    total_spent_seconds = 0
    session_count = 1

    for event in sorted_events[1:]:
        current_time = event["time"]
        gap = int((current_time - last_time).total_seconds())
        if gap < 0:
            gap = 0

        action = event.get("action")
        new_session = False

        if action in SESSION_BREAK_ACTIONS:
            new_session = True
        elif gap > threshold_seconds:
            is_video_event = event.get("source") == "video"
            if not is_video_event:
                new_session = True

        if new_session:
            duration = int((last_time - session_start).total_seconds())
            if duration > 0:
                total_spent_seconds += duration
            session_start = current_time
            session_count += 1

        last_time = current_time

    final_duration = int((last_time - session_start).total_seconds())
    if final_duration > 0:
        total_spent_seconds += final_duration

    return {
        "total_seconds": total_spent_seconds,
        "session_count": session_count,
        "event_count": len(sorted_events),
    }


def split_before_date(events, split_at):
    before = []
    for event in events:
        if event["time"] < split_at:
            before.append(event)
    return before


def calculate_connect_disconnect(events):
    if not events:
        return {
            "total_seconds": 0,
            "pair_count": 0,
            "open_connection_count": 0,
            "anomaly_count": 0,
            "event_count": 0,
        }

    sorted_events = sorted(events, key=lambda item: (item["time"], item["event_id"]))
    open_connections = {}
    total_seconds = 0
    pair_count = 0
    anomaly_count = 0
    processed_event_count = 0

    for event in sorted_events:
        action = event.get("action")
        if action not in {"Connect", "Disconnect"}:
            continue

        processed_event_count += 1
        session_id = event.get("session_id")
        connection_id = event.get("connection_id")
        if not session_id or not connection_id:
            anomaly_count += 1
            continue

        key = f"{session_id}|{connection_id}"
        event_time = event["time"]

        if action == "Connect":
            if key in open_connections:
                anomaly_count += 1
            open_connections[key] = event_time
            continue

        start_time = open_connections.pop(key, None)
        if start_time is None:
            anomaly_count += 1
            continue

        duration = int((event_time - start_time).total_seconds())
        if duration < 0:
            anomaly_count += 1
            continue

        total_seconds += duration
        pair_count += 1

    return {
        "total_seconds": total_seconds,
        "pair_count": pair_count,
        "open_connection_count": len(open_connections),
        "anomaly_count": anomaly_count,
        "event_count": processed_event_count,
    }
