from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    PROJECT_NAME: str = "QueryMind"
    API_V1_STR: str = "/api/v1"
    SECRET_KEY: str = "changethis"

    # CORS
    CORS_ORIGINS: list[str] = ["http://localhost:3000"]

    # Database (Neon)
    DATABASE_URL: str = ""

    # Google AI
    GOOGLE_API_KEY: str = ""

    # Pinecone
    PINECONE_API_KEY: str = ""
    PINECONE_INDEX_NAME: str = "querymind-schema"

    # Security — Fernet key (generate: Fernet.generate_key().decode())
    ENCRYPTION_KEY: str = ""

    # Clerk Auth
    CLERK_ISSUER: str = ""
    CLERK_JWKS_URL: str = ""

    model_config = SettingsConfigDict(
        env_file=".env",
        case_sensitive=True,
        extra="ignore",
    )


settings = Settings()
