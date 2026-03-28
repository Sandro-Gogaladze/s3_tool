import json
import logging

from botocore.exceptions import ClientError

logger = logging.getLogger(__name__)


def list_buckets(aws_s3_client):
    logger.info("Listing buckets")
    try:
        response = aws_s3_client.list_buckets()
        logger.info("Retrieved %s buckets", len(response.get("Buckets", [])))
        return response
    except ClientError:
        logger.exception("Failed to list buckets")
        return False


def create_bucket(aws_s3_client, bucket_name: str, region: str = "us-west-2"):
    logger.info("Creating bucket '%s' in region '%s'", bucket_name, region)
    try:
        if region == "us-east-1":
            # us-east-1 is the default region and must NOT include
            # CreateBucketConfiguration — passing it raises an error.
            response = aws_s3_client.create_bucket(Bucket=bucket_name)
        else:
            response = aws_s3_client.create_bucket(
                Bucket=bucket_name,
                CreateBucketConfiguration={"LocationConstraint": region},
            )
    except ClientError:
        logger.exception("Failed to create bucket '%s'", bucket_name)
        return False
    status_code = response["ResponseMetadata"]["HTTPStatusCode"]
    logger.info("Create bucket status code: %s", status_code)
    return status_code == 200


def delete_bucket(aws_s3_client, bucket_name: str):
    logger.info("Deleting bucket '%s'", bucket_name)
    try:
        response = aws_s3_client.delete_bucket(Bucket=bucket_name)
    except ClientError:
        logger.exception("Failed to delete bucket '%s'", bucket_name)
        return False
    status_code = response["ResponseMetadata"]["HTTPStatusCode"]
    logger.info("Delete bucket status code: %s", status_code)
    return status_code == 200


def bucket_exists(aws_s3_client, bucket_name: str):
    logger.info("Checking if bucket '%s' exists", bucket_name)
    try:
        response = aws_s3_client.head_bucket(Bucket=bucket_name)
    except ClientError:
        logger.exception("Bucket '%s' does not exist or is inaccessible", bucket_name)
        return False
    status_code = response["ResponseMetadata"]["HTTPStatusCode"]
    logger.info("Head bucket status code: %s", status_code)
    return status_code == 200


def set_object_access_policy(aws_s3_client, bucket_name: str, file_name: str):
    logger.info("Setting object ACL public-read for '%s/%s'", bucket_name, file_name)
    try:
        response = aws_s3_client.put_object_acl(ACL="public-read", Bucket=bucket_name, Key=file_name)
    except ClientError:
        logger.exception("Failed to set object ACL")
        return False
    status_code = response["ResponseMetadata"]["HTTPStatusCode"]
    logger.info("Put object ACL status code: %s", status_code)
    return status_code == 200


def generate_public_read_policy(bucket_name: str) -> str:
    logger.info("Generating public read policy for bucket '%s'", bucket_name)
    policy = {
        "Version": "2012-10-17",
        "Statement": [
            {
                "Sid": "PublicReadGetObject",
                "Effect": "Allow",
                "Principal": "*",
                "Action": "s3:GetObject",
                "Resource": f"arn:aws:s3:::{bucket_name}/*",
            }
        ],
    }
    return json.dumps(policy)


def create_bucket_policy(aws_s3_client, bucket_name: str):
    logger.info("Creating bucket policy for '%s'", bucket_name)
    try:
        # Block public access must be removed before a public-read policy can be applied.
        aws_s3_client.delete_public_access_block(Bucket=bucket_name)
        aws_s3_client.put_bucket_policy(Bucket=bucket_name, Policy=generate_public_read_policy(bucket_name))
        logger.info("Bucket policy created successfully for '%s'", bucket_name)
        return True
    except ClientError:
        logger.exception("Failed to create bucket policy for '%s'", bucket_name)
        return False


def read_bucket_policy(aws_s3_client, bucket_name: str):
    logger.info("Reading bucket policy for '%s'", bucket_name)
    try:
        policy = aws_s3_client.get_bucket_policy(Bucket=bucket_name)
        policy_str = policy["Policy"]
        logger.info("Bucket policy: %s", policy_str)
        return policy_str
    except ClientError:
        logger.exception("Failed to read bucket policy for '%s'", bucket_name)
        return False


def get_versioning_status(aws_s3_client, bucket_name: str) -> str:
    """Return the versioning status of a bucket: Enabled, Suspended, or Disabled."""
    logger.info("Checking versioning status for bucket '%s'", bucket_name)
    try:
        response = aws_s3_client.get_bucket_versioning(Bucket=bucket_name)
    except ClientError:
        logger.exception("Failed to get versioning status for '%s'", bucket_name)
        raise
    # Status is absent from the response when versioning has never been enabled.
    status = response.get("Status", "Disabled")
    logger.info("Versioning status for '%s': %s", bucket_name, status)
    return status


def list_object_versions(aws_s3_client, bucket_name: str, key: str) -> list:
    """Return all versions of an object, sorted newest first.

    Each entry is a dict with VersionId, LastModified, and IsLatest.
    """
    logger.info("Listing versions for '%s' in bucket '%s'", key, bucket_name)
    try:
        response = aws_s3_client.list_object_versions(Bucket=bucket_name, Prefix=key)
    except ClientError:
        logger.exception("Failed to list versions for '%s'", key)
        raise
    versions = response.get("Versions", [])
    # Filter to exact key match (Prefix can return keys that merely start with `key`)
    versions = [v for v in versions if v["Key"] == key]
    versions.sort(key=lambda v: v["LastModified"], reverse=True)
    logger.info("Found %s version(s) for '%s'", len(versions), key)
    for v in versions:
        logger.info("  VersionId: %s | Created: %s | Latest: %s",
                    v["VersionId"], v["LastModified"], v["IsLatest"])
    return versions


def restore_previous_version(aws_s3_client, bucket_name: str, key: str) -> bool:
    """Copy the previous version of an object on top of the current one.

    This creates a new version whose content matches the second-most-recent
    version, effectively rolling back one step without deleting any history.
    """
    logger.info("Restoring previous version of '%s' in bucket '%s'", key, bucket_name)
    versions = list_object_versions(aws_s3_client, bucket_name, key)

    if len(versions) < 2:
        logger.info("No previous version to restore for '%s' (only %s version(s) exist)", key, len(versions))
        return False

    # versions[0] is the current (latest), versions[1] is the one before it.
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


def delete_object(aws_s3_client, bucket_name: str, key: str) -> bool:
    logger.info("Deleting object '%s' from bucket '%s'", key, bucket_name)
    try:
        response = aws_s3_client.delete_object(Bucket=bucket_name, Key=key)
    except ClientError:
        logger.exception("Failed to delete object '%s' from bucket '%s'", key, bucket_name)
        return False
    status_code = response["ResponseMetadata"]["HTTPStatusCode"]
    logger.info("delete_object status code: %s", status_code)
    return status_code == 204
