from pydantic_settings import BaseSettings
from functools import lru_cache
from pathlib import Path

class Settings(BaseSettings):
    APP_NAME: str = "Games Analytics API"
    APP_VERSION: str = "1.0.0"
    CSV_PATH: str = "base_jogos.csv"  # default expects file at repo root
    ENABLE_DUCKDB: bool = True
    ENABLE_CONFIDENCE_WEIGHT: bool = True
    LOG_LEVEL: str = "INFO"
    OPENAI_API_KEY: str | None = None
    ALLOW_WEB_SEARCH: bool = False  # off by default
    ALLOW_WEB: bool = False

    class Config:
        env_file = ".env"

@lru_cache
def get_settings() -> Settings:
    return Settings()

def resolve_csv_path(p: str) -> Path:
    path = Path(p)
    if path.exists():
        return path
    # try common data path
    alt = Path(__file__).resolve().parents[2] / "data" / Path(p).name
    return alt

# avoid logging secrets
def mask(s: str | None) -> str:
    if not s:
        return ""
    if len(s) < 8:
        return "****"
    return s[:4] + "..." + s[-4:]

