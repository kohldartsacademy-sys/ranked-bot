import logging
import os
from pathlib import Path
import dotenv

logger = logging.getLogger("bot")
BASE_DIR = Path(__file__).resolve().parent


def _load_dotenv() -> None:
    candidates = [
        BASE_DIR / ".env",
        BASE_DIR / "env",
        BASE_DIR.parent / ".env",
    ]
    for candidate in candidates:
        if candidate.exists():
            dotenv.load_dotenv(candidate)
            return
    logger.warning(
        "No environment file found. Checked: %s",
        ", ".join(str(path) for path in candidates),
    )


def load_env(key: str, default: str) -> str:
    value = os.getenv(key)
    if value:
        return value
    logger.warning("Can't load env-variable for: '%s' - falling back to DEFAULT %s='%s'", key, key, default)
    return default

def load_int_env(key: str, default: int) -> int:
    value = os.getenv(key)
    if value:
        try:
            return int(value)
        except ValueError:
            logger.warning("Can't parse env-variable '%s' as int - falling back to DEFAULT %s=%s", key, key, default)
    else:
        logger.warning("Can't load env-variable for: '%s' - falling back to DEFAULT %s=%s", key, key, default)
    return default


_load_dotenv()
TOKEN = load_env("DISCORD_TOKEN", "unknown")
RESULT_CHANNEL = load_env("RESULT_CHANNEL", "unknown")

if TOKEN == "unknown":
    raise RuntimeError(
        "DISCORD_TOKEN is missing. Create config/.env (or config/env) and set DISCORD_TOKEN=<your bot token>."
    )
