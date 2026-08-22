from __future__ import annotations

from functools import lru_cache
from pathlib import Path
from zoneinfo import ZoneInfo

from pydantic import Field, SecretStr, field_validator
from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    model_config = SettingsConfigDict(
        env_file=".env",
        env_file_encoding="utf-8",
        extra="ignore",
        case_sensitive=False,
    )

    app_env: str = "local"
    log_level: str = "INFO"
    scheduler_enabled: bool = False
    ingestion_time: str = "05:00"
    ingestion_timezone: str = "Asia/Ho_Chi_Minh"

    levelup_base_url: str = ""
    levelup_auth_path: str = "/authenticate"
    levelup_courses_path: str = "/courses"
    levelup_username: SecretStr = Field(default=SecretStr(""))
    levelup_password: SecretStr = Field(default=SecretStr(""))
    levelup_api_key: SecretStr = Field(default=SecretStr(""))
    levelup_api_version: str = "2"
    levelup_page_size: int = Field(default=1000, ge=1, le=1000)
    levelup_max_concurrency: int = Field(default=5, ge=1, le=100)
    levelup_lock_ttl_seconds: int = Field(default=3600, ge=30)

    http_max_retries: int = Field(default=3, ge=0, le=10)
    http_connect_timeout_seconds: float = Field(default=10, gt=0)
    http_read_timeout_seconds: float = Field(default=60, gt=0)

    bronze_storage_type: str = "local"
    bronze_local_path: Path = Path("./data/bronze")
    checkpoint_db_path: Path = Path("./data/state/ingestion.db")
    checkpoint_retention_days: int = Field(default=30, ge=1)

    @field_validator("ingestion_time")
    @classmethod
    def validate_ingestion_time(cls, value: str) -> str:
        try:
            hour, minute = (int(part) for part in value.split(":"))
        except (TypeError, ValueError) as exc:
            raise ValueError("INGESTION_TIME must use HH:MM") from exc
        if not 0 <= hour <= 23 or not 0 <= minute <= 59:
            raise ValueError("INGESTION_TIME must use a valid 24-hour time")
        return f"{hour:02d}:{minute:02d}"

    @field_validator("ingestion_timezone")
    @classmethod
    def validate_timezone(cls, value: str) -> str:
        ZoneInfo(value)
        return value

    @field_validator("bronze_storage_type")
    @classmethod
    def validate_storage_type(cls, value: str) -> str:
        if value.lower() != "local":
            raise ValueError("Only local Bronze storage is implemented; OneLake is not configured")
        return value.lower()

    @property
    def scheduler_may_run(self) -> bool:
        return self.scheduler_enabled and self.app_env.lower() != "test"

    def levelup_secrets(self) -> tuple[str, ...]:
        return tuple(
            secret
            for secret in (
                self.levelup_username.get_secret_value(),
                self.levelup_password.get_secret_value(),
                self.levelup_api_key.get_secret_value(),
            )
            if secret
        )

    def validate_levelup_runtime(self) -> None:
        missing = []
        if not self.levelup_base_url:
            missing.append("LEVELUP_BASE_URL")
        if not self.levelup_username.get_secret_value():
            missing.append("LEVELUP_USERNAME")
        if not self.levelup_password.get_secret_value():
            missing.append("LEVELUP_PASSWORD")
        if not self.levelup_api_key.get_secret_value():
            missing.append("LEVELUP_API_KEY")
        if missing:
            raise ValueError(f"Missing LevelUP configuration: {', '.join(missing)}")


@lru_cache
def get_settings() -> Settings:
    return Settings()
