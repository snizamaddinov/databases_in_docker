# Session Export

Exports session rows for a single user from `VISIT_COLLECTION` only.

Logic:
- Uses events with `Actn in [Connect, Disconnect]` and `serverTime >= METHOD2_SWITCH_AT`.
- Groups by `sessionId`.
- For each `sessionId`, takes:
  - first `Connect` as session start
  - last `Disconnect` as session end
- Does **not** use `connectionId`.

## Run

```bash
uv run python session_exports/export_user_sessions.py --username firat.yuksel5
```

Or by user id:

```bash
uv run python session_exports/export_user_sessions.py --user-id 1361882
```

Optional custom switch date:

```bash
uv run python session_exports/export_user_sessions.py \
  --username firat.yuksel5 \
  --switch-at 2026-01-05T00:00:00Z
```

Outputs are written to `session_exports/data/` as:
- `<prefix>_sessions.csv`
- `<prefix>_sessions.json`
