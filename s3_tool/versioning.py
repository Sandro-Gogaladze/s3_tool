import logging
from calendar import monthrange
from datetime import datetime, timezone

from botocore.exceptions import ClientError

logger = logging.getLogger(__name__)


def get_versioning_status(aws_s3_client, bucket_name: str) -> str:
    """Return the versioning status of a bucket: Enabled, Suspended, or Disabled."""
    logger.info("Checking versioning status for bucket '%s'", bucket_name)
    try:
        response = aws_s3_client.get_bucket_versioning(Bucket=bucket_name)
    except ClientError:
        logger.exception("Failed to get versioning status for '%s'", bucket_name)
        raise
    status = response.get("Status", "Disabled")
    logger.info("Versioning status for '%s': %s", bucket_name, status)
    return status


def list_object_versions(aws_s3_client, bucket_name: str, key: str) -> list:
    """Return all versions of an object, sorted newest first."""
    logger.info("Listing versions for '%s' in bucket '%s'", key, bucket_name)
    try:
        response = aws_s3_client.list_object_versions(Bucket=bucket_name, Prefix=key)
    except ClientError:
        logger.exception("Failed to list versions for '%s'", key)
        raise
    versions = response.get("Versions", [])
    versions = [v for v in versions if v["Key"] == key]
    versions.sort(key=lambda v: v["LastModified"], reverse=True)
    logger.info("Found %s version(s) for '%s'", len(versions), key)
    for version in versions:
        logger.info(
            "VersionId: %s | Created: %s | Latest: %s",
            version["VersionId"], version["LastModified"], version["IsLatest"],
        )
    return versions


def restore_previous_version(aws_s3_client, bucket_name: str, key: str) -> bool:
    """Copy the previous version of an object on top of the current one."""
    logger.info("Restoring previous version of '%s' in bucket '%s'", key, bucket_name)
    versions = list_object_versions(aws_s3_client, bucket_name, key)

    if len(versions) < 2:
        logger.info("No previous version to restore for '%s' (only %s version(s) exist)", key, len(versions))
        return False

    previous = versions[1]
    previous_version_id = previous["VersionId"]
    logger.info("Copying version '%s' (created %s) as new version", previous_version_id, previous["LastModified"])

    try:
        aws_s3_client.copy_object(
            Bucket=bucket_name,
            Key=key,
            CopySource={"Bucket": bucket_name, "Key": key, "VersionId": previous_version_id},
        )
    except ClientError:
        logger.exception("Failed to restore previous version of '%s'", key)
        raise
    logger.info("Previous version restored successfully as new latest for '%s'", key)
    return True


def _months_ago(now: datetime, months: int) -> datetime:
    month = now.month - months
    year = now.year
    while month <= 0:
        month += 12
        year -= 1
    day = min(now.day, monthrange(year, month)[1])
    return now.replace(year=year, month=month, day=day)


def delete_versions_older_than_six_months(aws_s3_client, bucket_name: str, keys: tuple[str, ...]) -> dict[str, int]:
    """Delete object versions older than six months for the given object keys."""
    cutoff = _months_ago(datetime.now(timezone.utc), 6)
    logger.info("Deleting versions created before %s in bucket '%s'", cutoff, bucket_name)

    deleted_counts: dict[str, int] = {}
    paginator = aws_s3_client.get_paginator("list_object_versions")

    for key in keys:
        deleted_counts[key] = 0
        logger.info("Checking versions for '%s'", key)

        try:
            for page in paginator.paginate(Bucket=bucket_name, Prefix=key):
                versions = [v for v in page.get("Versions", []) if v["Key"] == key]
                for version in versions:
                    version_id = version["VersionId"]
                    last_modified = version["LastModified"]
                    logger.info(
                        "VersionId: %s | Created: %s | Latest: %s",
                        version_id, last_modified, version["IsLatest"],
                    )
                    if last_modified < cutoff:
                        aws_s3_client.delete_object(Bucket=bucket_name, Key=key, VersionId=version_id)
                        deleted_counts[key] += 1
                        logger.info("Deleted old version '%s' for '%s'", version_id, key)
        except ClientError:
            logger.exception("Failed to delete old versions for '%s'", key)
            raise

        logger.info("Deleted %s old version(s) for '%s'", deleted_counts[key], key)

    return deleted_counts
