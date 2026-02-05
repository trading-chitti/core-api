"""Configuration for Core API Gateway."""

from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    """Application settings loaded from environment variables."""

    model_config = SettingsConfigDict(
        env_file=".env",
        env_file_encoding="utf-8",
        case_sensitive=True,
    )

    # TCP ports for services (no more Unix sockets!)
    MOJO_COMPUTE_HOST: str = "localhost"
    MOJO_COMPUTE_PORT: int = 6101  # Mojo compute service

    SIGNAL_SERVICE_HOST: str = "localhost"
    SIGNAL_SERVICE_PORT: int = 6002  # Signal-service HTTP port

    NEWS_NLP_HOST: str = "localhost"
    NEWS_NLP_PORT: int = 6102  # News NLP service

    # All services use TCP now (easier to track and debug)
    USE_UNIX_SOCKETS: bool = False

    # CORS settings
    CORS_ORIGINS: list[str] = [
        "http://localhost:5173",
        "http://localhost:3000",
        "http://localhost:6003",  # Dashboard (via proxy on 6003)
    ]

    # API settings
    API_PREFIX: str = "/api"
    DEBUG: bool = False

    # Zerodha Kite API credentials (must be set via environment variables)
    KITE_API_KEY: str = ""  # Set via KITE_API_KEY env var
    KITE_API_SECRET: str = ""  # Set via KITE_API_SECRET env var

    # Ind Money (IndStocks) API credentials
    INDMONEY_ENABLED: bool = False  # Set to True to enable Ind Money integration
    INDMONEY_ACCESS_TOKEN: str = ""  # Set via INDMONEY_ACCESS_TOKEN env var
    INDMONEY_USER_ID: str = ""  # Optional: Set via INDMONEY_USER_ID env var


settings = Settings()
