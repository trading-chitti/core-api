"""Configuration for Core API Gateway."""

from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    """Application settings loaded from environment variables."""

    model_config = SettingsConfigDict(
        env_file=".env",
        env_file_encoding="utf-8",
        case_sensitive=True,
    )

    # Socket paths for Mojo services
    MOJO_COMPUTE_SOCKET: str = "/tmp/mojo-compute.sock"
    SIGNAL_SERVICE_SOCKET: str = "/tmp/signal-service.sock"
    NEWS_NLP_SOCKET: str = "/tmp/news-nlp.sock"

    # For TCP sockets (if not using Unix sockets)
    MOJO_COMPUTE_HOST: str = "localhost"
    MOJO_COMPUTE_PORT: int = 6004

    SIGNAL_SERVICE_HOST: str = "localhost"
    SIGNAL_SERVICE_PORT: int = 6003

    NEWS_NLP_HOST: str = "localhost"
    NEWS_NLP_PORT: int = 6002

    # Use Unix sockets or TCP
    USE_UNIX_SOCKETS: bool = True

    # CORS settings
    CORS_ORIGINS: list[str] = ["http://localhost:5173", "http://localhost:3000"]

    # API settings
    API_PREFIX: str = "/api"
    DEBUG: bool = False


settings = Settings()
