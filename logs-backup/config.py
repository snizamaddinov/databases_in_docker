import os
from pathlib import Path

from dotenv import load_dotenv


BASE_DIR = Path(__file__).resolve().parent
load_dotenv(BASE_DIR / ".env")


def env_str(name, default=""):
    return os.getenv(name, default)


def env_int(name, default):
    raw = os.getenv(name)
    if raw is None or raw.strip() == "":
        return default
    return int(raw)


def env_bool(name, default):
    raw = os.getenv(name)
    if raw is None:
        return default
    return raw.strip().lower() in {"1", "true", "yes", "on"}


SEASON = env_str("SEASON", "2526")
USERS_JSON_PATH = env_str("USERS_JSON_PATH", f"users_{SEASON}.json")
LOG_DIR = env_str("LOG_DIR", "logs")
EXPORT_LOG_PATH = env_str("EXPORT_LOG_PATH", f"{LOG_DIR}/collect_users.log")
PROCESS_LOG_PATH = env_str("PROCESS_LOG_PATH", f"{LOG_DIR}/process_users.log")
UPDATE_LOG_PATH = env_str("UPDATE_LOG_PATH", f"{LOG_DIR}/update_users_spend_time.log")
os.makedirs(LOG_DIR, exist_ok=True)

PAGE_SIZE = env_int("PAGE_SIZE", 1000)
THRESHOLD_SECONDS = env_int("THRESHOLD_SECONDS", 15 * 60)
MIN_CREATED_AT = env_str("MIN_CREATED_AT", "2025-08-01T00:00:00Z")
PROCESS_ONLY_NEW = env_bool("PROCESS_ONLY_NEW", True)

MYSQL_HOST = env_str("MYSQL_HOST", "127.0.0.1")
MYSQL_PORT = env_int("MYSQL_PORT", 3306)
MYSQL_USER = env_str("MYSQL_USER", "root")
MYSQL_PASSWORD = env_str("MYSQL_PASSWORD", "")
MYSQL_DB = env_str("MYSQL_DB", "")

MYSQL_USER_TABLE = env_str("MYSQL_USER_TABLE", "user")
MYSQL_ID_COLUMN = env_str("MYSQL_ID_COLUMN", "id")
MYSQL_USERNAME_COLUMN = env_str("MYSQL_USERNAME_COLUMN", "username")
MYSQL_SPEND_TIME_COLUMN = env_str("MYSQL_SPEND_TIME_COLUMN", "spend_time")
MYSQL_SEASON_COLUMN = env_str("MYSQL_SEASON_COLUMN", "season")

MONGO_URI = env_str("MONGO_URI", "mongodb://127.0.0.1:27017")
MONGO_DB = env_str("MONGO_DB", "")
VISIT_COLLECTION = env_str("VISIT_COLLECTION", f"visit_log_{SEASON}")
VIDEO_COLLECTION = env_str("VIDEO_COLLECTION", f"video_log_{SEASON}")


