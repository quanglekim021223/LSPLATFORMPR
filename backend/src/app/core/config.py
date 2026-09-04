from __future__ import annotations

from datetime import datetime
from functools import lru_cache
from pathlib import Path
from typing import Literal
from zoneinfo import ZoneInfo

from pydantic import Field, SecretStr, field_validator
from pydantic_settings import BaseSettings, SettingsConfigDict

FAMS_ALLOWED_STATUSES = {
    "PLANNING",
    "ASSIGNED",
    "REVIEWING",
    "CANCELLED",
    "DECLINED",
    "INPROGRESS",
    "TRAINING_COMPLETED",
    "PENDING_CLOSE",
    "CLOSED",
}


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
    cors_allowed_origins: list[str] = Field(
        default_factory=lambda: [
            "http://localhost:5173",
            "http://127.0.0.1:5173",
        ]
    )

    auth_admin_username: str = ""
    auth_admin_password_hash: SecretStr = Field(default=SecretStr(""))
    auth_jwt_secret: SecretStr = Field(default=SecretStr(""))
    auth_token_expire_minutes: int = Field(default=60, ge=1, le=1440)

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

    linkedin_token_url: str = ""
    linkedin_base_url: str = ""
    linkedin_client_id: SecretStr = Field(default=SecretStr(""))
    linkedin_client_secret: SecretStr = Field(default=SecretStr(""))
    linkedin_page_size: int = Field(default=100, ge=1, le=100)
    linkedin_history_start_time: str = ""
    linkedin_max_concurrency: int = Field(default=5, ge=1, le=5)
    linkedin_asset_detail_query_template: str = ""
    linkedin_lock_ttl_seconds: int = Field(default=3600, ge=30)

    harvard_catalog_base_url: str = "https://catalog-api.myhbp.org/v1"
    harvard_page_size: int = Field(default=1000, ge=1, le=1000)
    harvard_hmm_client_id: SecretStr = Field(default=SecretStr(""))
    harvard_hmm_client_secret: SecretStr = Field(default=SecretStr(""))
    harvard_hmm_org_key: str = ""
    harvard_hmm_history_start_date: str = ""
    harvard_spark_client_id: SecretStr = Field(default=SecretStr(""))
    harvard_spark_client_secret: SecretStr = Field(default=SecretStr(""))
    harvard_spark_org_key: str = ""
    harvard_spark_history_start_date: str = ""
    harvard_sftp_host: str = "transfer.hbsp.harvard.edu"
    harvard_sftp_port: int = Field(default=22, ge=1, le=65535)
    harvard_sftp_username: SecretStr = Field(default=SecretStr(""))
    harvard_sftp_password: SecretStr = Field(default=SecretStr(""))
    harvard_sftp_remote_dir: str = "/fpt_sparkprod_feed"
    harvard_sftp_known_hosts: Path | None = None
    harvard_report_date_offset_days: int = Field(default=1, ge=0)
    harvard_sftp_poll_interval_seconds: int = Field(default=300, ge=1)
    harvard_sftp_max_wait_seconds: int = Field(default=7200, ge=0)
    harvard_sftp_max_retries: int = Field(default=3, ge=0, le=10)
    harvard_sftp_mock_enabled: bool = False

    fams_base_url: str = "https://fams.fa.edu.vn"
    fams_api_key: SecretStr = Field(default=SecretStr(""))
    fams_load_mode: Literal["full", "filtered"] = "full"
    fams_status: str = ""
    fams_site: str = ""
    fams_actual_start_date_from: str = ""
    fams_actual_start_date_to: str = ""
    fams_lock_ttl_seconds: int = Field(default=3600, ge=30)

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

    @field_validator("log_level")
    @classmethod
    def validate_log_level(cls, value: str) -> str:
        normalized = value.strip().upper()
        if normalized not in {"DEBUG", "INFO", "WARNING", "ERROR", "CRITICAL"}:
            raise ValueError("LOG_LEVEL must be DEBUG, INFO, WARNING, ERROR, or CRITICAL")
        return normalized

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

    @property
    def linkedin_configured(self) -> bool:
        return not self._missing_linkedin_configuration()

    def linkedin_secrets(self) -> tuple[str, ...]:
        return tuple(
            secret
            for secret in (
                self.linkedin_client_id.get_secret_value(),
                self.linkedin_client_secret.get_secret_value(),
            )
            if secret
        )

    @property
    def harvard_hmm_configured(self) -> bool:
        return not self._missing_harvard_configuration("hmm")

    @property
    def harvard_spark_configured(self) -> bool:
        return not self._missing_harvard_configuration("spark")

    def harvard_secrets(self, vendor: str) -> tuple[str, ...]:
        client_id = (
            self.harvard_hmm_client_id
            if vendor == "harvard_hmm"
            else self.harvard_spark_client_id
        )
        client_secret = (
            self.harvard_hmm_client_secret
            if vendor == "harvard_hmm"
            else self.harvard_spark_client_secret
        )
        return tuple(
            secret
            for secret in (
                client_id.get_secret_value(),
                client_secret.get_secret_value(),
                self.harvard_sftp_username.get_secret_value(),
                self.harvard_sftp_password.get_secret_value(),
            )
            if secret
        )

    @property
    def fams_configured(self) -> bool:
        return bool(self.fams_base_url and self.fams_api_key.get_secret_value())

    def fams_secrets(self) -> tuple[str, ...]:
        api_key = self.fams_api_key.get_secret_value()
        return (api_key,) if api_key else ()

    def validate_auth_runtime(self) -> None:
        missing = [
            name
            for name, value in (
                ("AUTH_ADMIN_USERNAME", self.auth_admin_username),
                (
                    "AUTH_ADMIN_PASSWORD_HASH",
                    self.auth_admin_password_hash.get_secret_value(),
                ),
                ("AUTH_JWT_SECRET", self.auth_jwt_secret.get_secret_value()),
            )
            if not value
        ]
        if missing:
            raise ValueError(f"Missing authentication configuration: {', '.join(missing)}")
        if len(self.auth_jwt_secret.get_secret_value()) < 32:
            raise ValueError("AUTH_JWT_SECRET must contain at least 32 characters")

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

    def validate_linkedin_runtime(self) -> None:
        missing = self._missing_linkedin_configuration()
        if missing:
            raise ValueError(f"Missing LinkedIn configuration: {', '.join(missing)}")

    def _missing_linkedin_configuration(self) -> list[str]:
        values = {
            "LINKEDIN_TOKEN_URL": self.linkedin_token_url,
            "LINKEDIN_BASE_URL": self.linkedin_base_url,
            "LINKEDIN_CLIENT_ID": self.linkedin_client_id.get_secret_value(),
            "LINKEDIN_CLIENT_SECRET": self.linkedin_client_secret.get_secret_value(),
            "LINKEDIN_HISTORY_START_TIME": self.linkedin_history_start_time,
            "LINKEDIN_ASSET_DETAIL_QUERY_TEMPLATE": (
                self.linkedin_asset_detail_query_template
            ),
        }
        return [name for name, value in values.items() if not value]

    def validate_harvard_runtime(self, vendor: str) -> None:
        short_name = "hmm" if vendor == "harvard_hmm" else "spark"
        missing = self._missing_harvard_configuration(short_name)
        if missing:
            display_name = "Harvard HMM" if short_name == "hmm" else "Harvard Spark"
            raise ValueError(
                f"Missing {display_name} configuration: {', '.join(missing)}"
            )

    def validate_harvard_catalog_runtime(self, vendor: str) -> None:
        short_name = "hmm" if vendor == "harvard_hmm" else "spark"
        missing = self._missing_harvard_catalog_configuration(short_name)
        if missing:
            display_name = "Harvard HMM" if short_name == "hmm" else "Harvard Spark"
            raise ValueError(
                f"Missing {display_name} Catalog configuration: {', '.join(missing)}"
            )

    def validate_harvard_sftp_runtime(self) -> None:
        missing = self._missing_harvard_sftp_configuration()
        if missing:
            raise ValueError(
                f"Missing Harvard SFTP configuration: {', '.join(missing)}"
            )

    def validate_fams_runtime(self) -> None:
        missing = self._missing_fams_configuration()
        if missing:
            raise ValueError(f"Missing FAMS configuration: {', '.join(missing)}")

        if self.fams_load_mode == "filtered":
            self._validate_fams_filters()

    def _missing_fams_configuration(self) -> list[str]:
        values = {
            "FAMS_BASE_URL": self.fams_base_url,
            "FAMS_API_KEY": self.fams_api_key.get_secret_value(),
        }
        return [name for name, value in values.items() if not value]

    def _validate_fams_filters(self) -> None:
        filters = (
            self.fams_status,
            self.fams_site,
            self.fams_actual_start_date_from,
            self.fams_actual_start_date_to,
        )
        if not any(value.strip() for value in filters):
            raise ValueError(
                "FAMS_LOAD_MODE=filtered requires at least one non-empty filter"
            )
        self._validate_fams_status()
        self._validate_fams_site()
        self._validate_fams_dates()

    def _validate_fams_status(self) -> None:
        if not self.fams_status:
            return
        statuses = [value.strip() for value in self.fams_status.split(",")]
        if any(not value or value not in FAMS_ALLOWED_STATUSES for value in statuses):
            raise ValueError(
                "FAMS_STATUS contains invalid or empty comma-separated values"
            )

    def _validate_fams_site(self) -> None:
        if not self.fams_site:
            return
        sites = [value.strip() for value in self.fams_site.split(",")]
        if any(not value for value in sites):
            raise ValueError("FAMS_SITE contains an empty comma-separated value")

    def _validate_fams_dates(self) -> None:
        for name, value in (
            ("FAMS_ACTUAL_START_DATE_FROM", self.fams_actual_start_date_from),
            ("FAMS_ACTUAL_START_DATE_TO", self.fams_actual_start_date_to),
        ):
            if value:
                try:
                    datetime.strptime(value, "%Y%m%d")
                except ValueError as exc:
                    raise ValueError(f"{name} must use YYYYMMDD") from exc

        if (
            self.fams_actual_start_date_from
            and self.fams_actual_start_date_to
            and self.fams_actual_start_date_from > self.fams_actual_start_date_to
        ):
            raise ValueError(
                "FAMS_ACTUAL_START_DATE_FROM must not be after "
                "FAMS_ACTUAL_START_DATE_TO"
            )

    def _missing_harvard_configuration(self, vendor: str) -> list[str]:
        return self._missing_harvard_catalog_configuration(
            vendor
        ) + self._missing_harvard_sftp_configuration()

    def _missing_harvard_catalog_configuration(self, vendor: str) -> list[str]:
        prefix = "HARVARD_HMM" if vendor == "hmm" else "HARVARD_SPARK"
        client_id = (
            self.harvard_hmm_client_id
            if vendor == "hmm"
            else self.harvard_spark_client_id
        )
        client_secret = (
            self.harvard_hmm_client_secret
            if vendor == "hmm"
            else self.harvard_spark_client_secret
        )
        org_key = (
            self.harvard_hmm_org_key
            if vendor == "hmm"
            else self.harvard_spark_org_key
        )
        values: dict[str, object] = {
            "HARVARD_CATALOG_BASE_URL": self.harvard_catalog_base_url,
            f"{prefix}_CLIENT_ID": client_id.get_secret_value(),
            f"{prefix}_CLIENT_SECRET": client_secret.get_secret_value(),
            f"{prefix}_ORG_KEY": org_key,
        }
        return [name for name, value in values.items() if not value]

    def _missing_harvard_sftp_configuration(self) -> list[str]:
        values: dict[str, object] = {
            "HARVARD_SFTP_HOST": self.harvard_sftp_host,
            "HARVARD_SFTP_USERNAME": self.harvard_sftp_username.get_secret_value(),
            "HARVARD_SFTP_PASSWORD": self.harvard_sftp_password.get_secret_value(),
            "HARVARD_SFTP_REMOTE_DIR": self.harvard_sftp_remote_dir,
            "HARVARD_SFTP_KNOWN_HOSTS": self.harvard_sftp_known_hosts,
        }
        return [name for name, value in values.items() if not value]


@lru_cache
def get_settings() -> Settings:
    return Settings()
