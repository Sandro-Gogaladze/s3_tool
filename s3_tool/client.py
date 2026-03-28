import logging
import os

import boto3
from botocore.exceptions import ClientError
from dotenv import load_dotenv

load_dotenv()

logger = logging.getLogger(__name__)


def _get_env(name: str) -> str | None:
    """Read an env var by its canonical name, falling back to the lower-cased variant.

    Allows both AWS_REGION_NAME and aws_region_name in .env files.
    """
    return os.getenv(name) or os.getenv(name.lower())


def init_client():
    logger.info("Initializing S3 client")
    try:
        client = boto3.client(
            "s3",
            aws_access_key_id=_get_env("AWS_ACCESS_KEY_ID"),
            aws_secret_access_key=_get_env("AWS_SECRET_ACCESS_KEY"),
            aws_session_token=_get_env("AWS_SESSION_TOKEN"),
            region_name=_get_env("AWS_REGION_NAME"),
        )
        # Eagerly call list_buckets to validate credentials before any real work.
        # A ClientError here means the keys are wrong or have no S3 permissions.
        client.list_buckets()
        logger.info("S3 client initialized successfully")
        return client
    except ClientError:
        logger.exception("Failed to initialize S3 client")
        raise
