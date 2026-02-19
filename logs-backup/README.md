# Spend Time Backfill

Scripts:
- `export_users_mysql.py`: exports users from MySQL (`season`) to JSON with `status=new`.
- `process_users_spend_time.py`: reads JSON users, fetches paginated Mongo visit/video logs (`createdAt > MIN_CREATED_AT`), computes `new_spend_time_seconds`, and writes back.

## Configuration

All DB credentials and common globals are read from `.env` through `config.py`.

Main globals:
- `SEASON`
- `PAGE_SIZE`
- `THRESHOLD_SECONDS`
- `MIN_CREATED_AT`
- `PROCESS_ONLY_NEW`
- `EXPORT_LOG_PATH`
- `PROCESS_LOG_PATH`

Copy `.env.example` values into `.env` and set real credentials.

## Install

```bash
uv sync
```

## Run

```bash
./.venv/bin/python export_users_mysql.py
./.venv/bin/python process_users_spend_time.py
```
