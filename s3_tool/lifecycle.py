import logging

from botocore.exceptions import ClientError

logger = logging.getLogger(__name__)


def set_lifecycle_policy(aws_s3_client, bucket_name: str, days: int = 120) -> bool:
    """Attach a lifecycle rule that auto-deletes ALL objects after `days` days.

    An empty Prefix in the Filter means the rule applies to every object in
    the bucket regardless of key name.
    """
    logger.info("Setting lifecycle policy on '%s': delete after %s days", bucket_name, days)
    try:
        response = aws_s3_client.put_bucket_lifecycle_configuration(
            Bucket=bucket_name,
            LifecycleConfiguration={
                "Rules": [
                    {
                        "ID": f"auto-delete-after-{days}-days",
                        "Status": "Enabled",
                        "Filter": {"Prefix": ""},  # empty prefix = applies to ALL objects
                        "Expiration": {"Days": days},
                    }
                ]
            },
        )
    except ClientError:
        logger.exception("Failed to set lifecycle policy on '%s'", bucket_name)
        raise
    status_code = response["ResponseMetadata"]["HTTPStatusCode"]
    logger.info("set_lifecycle_policy status code: %s", status_code)
    return status_code == 200


def read_lifecycle_policy(aws_s3_client, bucket_name: str) -> list:
    """Return the bucket's lifecycle rules, or an empty list if none are configured."""
    logger.info("Reading lifecycle policy for '%s'", bucket_name)
    try:
        response = aws_s3_client.get_bucket_lifecycle_configuration(Bucket=bucket_name)
        return response.get("Rules", [])
    except ClientError as e:
        # NoSuchLifecycleConfiguration is not an error — the bucket simply has no rules yet.
        if e.response["Error"]["Code"] == "NoSuchLifecycleConfiguration":
            logger.info("No lifecycle policy set on '%s'", bucket_name)
            return []
        logger.exception("Failed to read lifecycle policy for '%s'", bucket_name)
        raise
