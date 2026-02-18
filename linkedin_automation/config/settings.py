"""
Centralized configuration for the LinkedIn post automation pipeline.

Mirrors the dataclass pattern used in market_data_flow.py so all env-var
validation happens at import time. Failing fast here is intentional - a
missing API key discovered mid-run wastes an Anthropic call and leaves the
topic queue in an ambiguous state.
"""

import os
from dataclasses import dataclass

import dotenv

dotenv.load_dotenv()


@dataclass
class LinkedInConfig:
    """
    All runtime configuration read from environment variables.

    S3 paths for the topic CSVs are intentionally kept as constants rather
    than env vars - they are part of the application contract, not deployment
    configuration, so keeping them here avoids silent drift between deploys.
    """

    # Anthropic
    anthropic_api_key: str = os.getenv("ANTHROPIC_API_KEY", "")

    # AWS
    s3_bucket: str = os.getenv("S3_BUCKET", "")

    # Email delivery
    email_sender: str = os.getenv("EMAIL_SENDER", "")
    email_recipient: str = os.getenv("EMAIL_RECIPIENT", "")
    email_password: str = os.getenv("EMAIL_PASSWORD", "")
    smtp_host: str = os.getenv("SMTP_HOST", "smtp.gmail.com")

    # smtp_port is stored as str so the dataclass default can be set inline;
    # __post_init__ converts it to int after env-var substitution.
    smtp_port: str = os.getenv("SMTP_PORT", "587")

    # S3 key paths - fixed so the topic queue location never drifts
    topics_s3_key: str = "linkedin_automation/data/topics.csv"
    used_topics_s3_key: str = "linkedin_automation/data/used_topics.csv"

    # Pinned model ID so changes require an explicit code review
    anthropic_model: str = "claude-sonnet-4-6"

    def __post_init__(self) -> None:
        # Convert port to int now that all fields are populated
        self.smtp_port = int(self.smtp_port)

        missing: list[str] = []
        if not self.anthropic_api_key:
            missing.append("ANTHROPIC_API_KEY")
        if not self.s3_bucket:
            missing.append("S3_BUCKET")
        if not self.email_sender:
            missing.append("EMAIL_SENDER")
        if not self.email_recipient:
            missing.append("EMAIL_RECIPIENT")
        if not self.email_password:
            missing.append("EMAIL_PASSWORD")

        if missing:
            raise ValueError(
                f"Missing required environment variables: {', '.join(missing)}"
            )
