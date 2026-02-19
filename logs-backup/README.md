# Spend Time Backfill

Scripts:
- `export_users_mysql.py`: exports users from MySQL (`season`) to JSON with `status=new`.
- `process_users_spend_time.py`: reads JSON users, fetches paginated Mongo visit/video logs (`createdAt > MIN_CREATED_AT`), computes `new_spend_time_seconds`, and writes back.
- `update_users_spend_time.py`: reads JSON users and updates MySQL `spend_time` from `new_spend_time_seconds`.
- `compare_spend_time_methods.py`: computes multi-threshold comparisons for:
  - Method 1: event-gap based calculation on merged visit/video events.
  - Method 2: event-gap before `METHOD2_SWITCH_AT` + `Connect/Disconnect` pairing after switch date.

## Configuration

All DB credentials and common globals are read from `.env` through `config.py`.

Main globals:
- `SEASON`
- `LOG_DIR`
- `PAGE_SIZE`
- `THRESHOLD_SECONDS`
- `THRESHOLD_VALUES`
- `MIN_CREATED_AT`
- `METHOD2_SWITCH_AT`
- `PROCESS_ONLY_NEW`
- `EXPORT_LOG_PATH`
- `PROCESS_LOG_PATH`
- `UPDATE_LOG_PATH`
- `COMPARE_LOG_PATH`
- `COMPARE_OUTPUT_PATH`
- `EVENTS_COLLECTION`

Copy `.env.example` values into `.env` and set real credentials.

## Install

```bash
uv sync
```

## Run

```bash
./.venv/bin/python export_users_mysql.py
./.venv/bin/python process_users_spend_time.py
./.venv/bin/python update_users_spend_time.py
./.venv/bin/python compare_spend_time_methods.py
```
