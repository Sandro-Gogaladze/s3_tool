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
