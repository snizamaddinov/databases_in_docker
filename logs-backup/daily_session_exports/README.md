# Daily Session Export

Exports per-session rows for one user from a local Mongo JSON export file, using the existing spend-time method logic:

- Pre-switch sessions: `method1` with gap threshold (default `300` seconds).
- Post-switch sessions: `Connect`/`Disconnect` pairing logic.

No Mongo connection is used.

## Run

```bash
cp matomo.visit_log_2526.json daily_session_exports/data/matomo.visit_log_2526.json
uv run python daily_session_exports/export_daily_sessions.py \
  --username ahmet.aslan.biyik
```

Optional parameters:

```bash
uv run python daily_session_exports/export_daily_sessions.py \
  --username ahmet.aslan.biyik \
  --switch-at 2026-01-05T00:00:00Z \
  --threshold-seconds 300 \
  --output-prefix ahmet_aslan_biyik
```

Outputs are written to `daily_session_exports/data/`:

- `<prefix>_daily_sessions.csv`
- `<prefix>_daily_sessions.json`
