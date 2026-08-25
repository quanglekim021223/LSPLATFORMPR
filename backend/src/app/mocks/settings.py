from __future__ import annotations

from functools import lru_cache

from pydantic import SecretStr
from pydantic_settings import BaseSettings, SettingsConfigDict


class MockSettings(BaseSettings):
    """Credentials owned by the local mock vendor side."""

    model_config = SettingsConfigDict(
        env_file=".env",
        env_file_encoding="utf-8",
        extra="ignore",
        case_sensitive=False,
    )

    mock_levelup_username: SecretStr = SecretStr("")
    mock_levelup_password: SecretStr = SecretStr("")
    mock_levelup_api_key: SecretStr = SecretStr("")
    mock_levelup_api_version: str = "2"
    mock_levelup_access_token: SecretStr = SecretStr("")

    mock_skillup_api_key: SecretStr = SecretStr("")
    mock_datacamp_token: SecretStr = SecretStr("")

    mock_coursera_username: SecretStr = SecretStr("")
    mock_coursera_password: SecretStr = SecretStr("")
    mock_coursera_org_id: str = ""
    mock_coursera_access_token: SecretStr = SecretStr("")

    mock_linkedin_client_id: SecretStr = SecretStr("")
    mock_linkedin_client_secret: SecretStr = SecretStr("")
    mock_linkedin_access_token: SecretStr = SecretStr("")

    mock_harvard_hmm_client_id: SecretStr = SecretStr("")
    mock_harvard_hmm_client_secret: SecretStr = SecretStr("")
    mock_harvard_hmm_org_key: str = ""
    mock_harvard_hmm_access_token: SecretStr = SecretStr("")
    mock_harvard_spark_client_id: SecretStr = SecretStr("")
    mock_harvard_spark_client_secret: SecretStr = SecretStr("")
    mock_harvard_spark_org_key: str = ""
    mock_harvard_spark_access_token: SecretStr = SecretStr("")
    mock_harvard_sftp_host: str = ""
    mock_harvard_sftp_username: SecretStr = SecretStr("")
    mock_harvard_sftp_password: SecretStr = SecretStr("")
    mock_harvard_sftp_host_key: str = ""

    mock_fams_api_key: SecretStr = SecretStr("")

    def validate_runtime(self) -> None:
        values: dict[str, str] = {
            "MOCK_LEVELUP_USERNAME": self.mock_levelup_username.get_secret_value(),
            "MOCK_LEVELUP_PASSWORD": self.mock_levelup_password.get_secret_value(),
            "MOCK_LEVELUP_API_KEY": self.mock_levelup_api_key.get_secret_value(),
            "MOCK_LEVELUP_ACCESS_TOKEN": (
                self.mock_levelup_access_token.get_secret_value()
            ),
            "MOCK_SKILLUP_API_KEY": self.mock_skillup_api_key.get_secret_value(),
            "MOCK_DATACAMP_TOKEN": self.mock_datacamp_token.get_secret_value(),
            "MOCK_COURSERA_USERNAME": (
                self.mock_coursera_username.get_secret_value()
            ),
            "MOCK_COURSERA_PASSWORD": (
                self.mock_coursera_password.get_secret_value()
            ),
            "MOCK_COURSERA_ORG_ID": self.mock_coursera_org_id,
            "MOCK_COURSERA_ACCESS_TOKEN": (
                self.mock_coursera_access_token.get_secret_value()
            ),
            "MOCK_LINKEDIN_CLIENT_ID": (
                self.mock_linkedin_client_id.get_secret_value()
            ),
            "MOCK_LINKEDIN_CLIENT_SECRET": (
                self.mock_linkedin_client_secret.get_secret_value()
            ),
            "MOCK_LINKEDIN_ACCESS_TOKEN": (
                self.mock_linkedin_access_token.get_secret_value()
            ),
            "MOCK_HARVARD_HMM_CLIENT_ID": (
                self.mock_harvard_hmm_client_id.get_secret_value()
            ),
            "MOCK_HARVARD_HMM_CLIENT_SECRET": (
                self.mock_harvard_hmm_client_secret.get_secret_value()
            ),
            "MOCK_HARVARD_HMM_ORG_KEY": self.mock_harvard_hmm_org_key,
            "MOCK_HARVARD_HMM_ACCESS_TOKEN": (
                self.mock_harvard_hmm_access_token.get_secret_value()
            ),
            "MOCK_HARVARD_SPARK_CLIENT_ID": (
                self.mock_harvard_spark_client_id.get_secret_value()
            ),
            "MOCK_HARVARD_SPARK_CLIENT_SECRET": (
                self.mock_harvard_spark_client_secret.get_secret_value()
            ),
            "MOCK_HARVARD_SPARK_ORG_KEY": self.mock_harvard_spark_org_key,
            "MOCK_HARVARD_SPARK_ACCESS_TOKEN": (
                self.mock_harvard_spark_access_token.get_secret_value()
            ),
            "MOCK_HARVARD_SFTP_HOST": self.mock_harvard_sftp_host,
            "MOCK_HARVARD_SFTP_USERNAME": (
                self.mock_harvard_sftp_username.get_secret_value()
            ),
            "MOCK_HARVARD_SFTP_PASSWORD": (
                self.mock_harvard_sftp_password.get_secret_value()
            ),
            "MOCK_HARVARD_SFTP_HOST_KEY": self.mock_harvard_sftp_host_key,
            "MOCK_FAMS_API_KEY": self.mock_fams_api_key.get_secret_value(),
        }
        missing = [name for name, value in values.items() if not value]
        if missing:
            raise ValueError(f"Missing mock vendor configuration: {', '.join(missing)}")


@lru_cache
def get_mock_settings() -> MockSettings:
    return MockSettings()
