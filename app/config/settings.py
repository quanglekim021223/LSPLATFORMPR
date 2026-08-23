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

    skillup_intelligence_base_url: str = "https://api.skillsintelligence.imocha.io"
    skillup_reports_base_url: str = "https://apiv3.imocha.io"
    skillup_api_key: SecretStr = Field(default=SecretStr(""))
    skillup_page_size: int = Field(default=100, ge=1, le=100)
    skillup_assessment_start_date: str = "2000-01-01T00:00:00Z"

    datacamp_base_url: str = ""
    datacamp_token: SecretStr = Field(default=SecretStr(""))
    datacamp_events_page_size: int = Field(default=1000, ge=1, le=1000)

    coursera_token_url: str = ""
    coursera_base_url: str = ""
    coursera_username: SecretStr = Field(default=SecretStr(""))
    coursera_password: SecretStr = Field(default=SecretStr(""))
    coursera_org_id: str = ""
    coursera_content_detail_path_template: str = ""
    coursera_page_size: int = Field(default=100, ge=1, le=1000)
    coursera_max_concurrency: int = Field(default=5, ge=1, le=5)
    coursera_lock_ttl_seconds: int = Field(default=3600, ge=30)

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

    @property
    def levelup_configured(self) -> bool:
        return bool(
            self.levelup_base_url
            and self.levelup_username.get_secret_value()
            and self.levelup_password.get_secret_value()
            and self.levelup_api_key.get_secret_value()
        )

    @property
    def skillup_configured(self) -> bool:
        return bool(
            self.skillup_intelligence_base_url
            and self.skillup_reports_base_url
            and self.skillup_api_key.get_secret_value()
        )

    def skillup_secrets(self) -> tuple[str, ...]:
        api_key = self.skillup_api_key.get_secret_value()
        return (api_key,) if api_key else ()

    @property
    def datacamp_configured(self) -> bool:
        return bool(self.datacamp_base_url and self.datacamp_token.get_secret_value())

    def datacamp_secrets(self) -> tuple[str, ...]:
        token = self.datacamp_token.get_secret_value()
        return (token,) if token else ()

    @property
    def coursera_configured(self) -> bool:
        return not self._missing_coursera_configuration()

    def coursera_secrets(self) -> tuple[str, ...]:
        return tuple(
            secret
            for secret in (
                self.coursera_username.get_secret_value(),
                self.coursera_password.get_secret_value(),
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

    def validate_skillup_runtime(self) -> None:
        missing = []
        if not self.skillup_intelligence_base_url:
            missing.append("SKILLUP_INTELLIGENCE_BASE_URL")
        if not self.skillup_reports_base_url:
            missing.append("SKILLUP_REPORTS_BASE_URL")
        if not self.skillup_api_key.get_secret_value():
            missing.append("SKILLUP_API_KEY")
        if missing:
            raise ValueError(f"Missing SkillUp configuration: {', '.join(missing)}")

    def validate_datacamp_runtime(self) -> None:
        missing = []
        if not self.datacamp_base_url:
            missing.append("DATACAMP_BASE_URL")
        if not self.datacamp_token.get_secret_value():
            missing.append("DATACAMP_TOKEN")
        if missing:
            raise ValueError(f"Missing DataCamp configuration: {', '.join(missing)}")

    def validate_coursera_runtime(self) -> None:
        missing = self._missing_coursera_configuration()
        if missing:
            raise ValueError(f"Missing Coursera configuration: {', '.join(missing)}")

    def _missing_coursera_configuration(self) -> list[str]:
        values = {
            "COURSERA_TOKEN_URL": self.coursera_token_url,
            "COURSERA_BASE_URL": self.coursera_base_url,
            "COURSERA_USERNAME": self.coursera_username.get_secret_value(),
            "COURSERA_PASSWORD": self.coursera_password.get_secret_value(),
            "COURSERA_ORG_ID": self.coursera_org_id,
            "COURSERA_CONTENT_DETAIL_PATH_TEMPLATE": (
                self.coursera_content_detail_path_template
            ),
        }
        return [name for name, value in values.items() if not value]


@lru_cache
def get_settings() -> Settings:
    return Settings()
