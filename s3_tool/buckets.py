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


def organize_by_extension(aws_s3_client, bucket_name: str) -> dict[str, int]:
    """Move every object in the bucket into a folder named after its extension.

    For example:
      image.jpg  -> jpg/image.jpg
      demo.csv   -> csv/demo.csv

    Objects that are already inside a folder (key contains '/') are skipped to
    avoid double-moving on repeated runs. Files with no extension go into
    'no_extension/'.

    Returns a dict mapping each extension to the number of files moved,
    e.g. {'jpg': 1, 'csv': 2}.
    """
    logger.info("Organizing objects by extension in bucket '%s'", bucket_name)

    # Collect all object keys via paginated listing
    keys = []
    paginator = aws_s3_client.get_paginator("list_objects_v2")
    for page in paginator.paginate(Bucket=bucket_name):
        for obj in page.get("Contents", []):
            keys.append(obj["Key"])

    logger.info("Found %s object(s) in bucket '%s'", len(keys), bucket_name)

    counts: dict[str, int] = {}

    for key in keys:
        # Skip objects that are already inside a folder
        if "/" in key:
            logger.info("Skipping '%s' (already in a folder)", key)
            continue

        file_name = key
        ext = file_name.rsplit(".", 1)[-1].lower() if "." in file_name else "no_extension"
        destination = f"{ext}/{file_name}"

        logger.info("Moving '%s' -> '%s'", key, destination)

        # S3 has no native move: copy then delete
        try:
            aws_s3_client.copy_object(
                Bucket=bucket_name,
                Key=destination,
                CopySource={"Bucket": bucket_name, "Key": key},
            )
            aws_s3_client.delete_object(Bucket=bucket_name, Key=key)
        except ClientError:
            logger.exception("Failed to move '%s' to '%s'", key, destination)
            raise

        counts[ext] = counts.get(ext, 0) + 1

    # Log the summary
    logger.info("Organization complete. Files moved per extension:")
    for ext, count in sorted(counts.items()):
        logger.info("  %s - %s", ext, count)

    return counts


def disable_public_access_block(aws_s3_client, bucket_name: str) -> bool:
    """Remove the Block Public Access settings so bucket policies can allow public reads."""
    logger.info("Disabling Block Public Access for bucket '%s'", bucket_name)
    try:
        aws_s3_client.delete_public_access_block(Bucket=bucket_name)
    except ClientError:
        logger.exception("Failed to disable public access block for '%s'", bucket_name)
        return False
    logger.info("Block Public Access disabled for '%s'", bucket_name)
    return True


def configure_website(aws_s3_client, bucket_name: str, index_document: str = "index.html", error_document: str = "error.html") -> bool:
    """Enable static website hosting on a bucket."""
    logger.info("Configuring website hosting on bucket '%s'", bucket_name)
    try:
        aws_s3_client.put_bucket_website(
            Bucket=bucket_name,
            WebsiteConfiguration={
                "IndexDocument": {"Suffix": index_document},
                "ErrorDocument": {"Key": error_document},
            },
        )
    except ClientError:
        logger.exception("Failed to configure website for '%s'", bucket_name)
        return False
    logger.info("Website hosting configured for '%s'", bucket_name)
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
