import io
import json
import logging
import os
from pathlib import Path
from urllib.request import urlopen

import boto3
import click
import filetype
from botocore.exceptions import ClientError
from dotenv import load_dotenv

load_dotenv()

logger = logging.getLogger(__name__)

ALLOWED_EXTENSIONS = {".bmp", ".jpg", ".jpeg", ".png", ".webp", ".mp4"}
ALLOWED_MIME_TYPES = {
    "image/bmp",
    "image/jpeg",
    "image/png",
    "image/webp",
    "video/mp4",
}


def _get_env(name: str) -> str | None:
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
        client.list_buckets()
        logger.info("S3 client initialized successfully")
        return client
    except ClientError:
        logger.exception("Failed to initialize S3 client")
        raise


def list_buckets(aws_s3_client):
    logger.info("Listing buckets")
    try:
        response = aws_s3_client.list_buckets()
        logger.info("Retrieved %s buckets", len(response.get("Buckets", [])))
        return response
    except ClientError:
        logger.exception("Failed to list buckets")
        return False


def create_bucket(aws_s3_client, bucket_name, region="us-west-2"):
    logger.info("Creating bucket '%s' in region '%s'", bucket_name, region)
    try:
        if region == "us-east-1":
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


def delete_bucket(aws_s3_client, bucket_name):
    logger.info("Deleting bucket '%s'", bucket_name)
    try:
        response = aws_s3_client.delete_bucket(Bucket=bucket_name)
    except ClientError:
        logger.exception("Failed to delete bucket '%s'", bucket_name)
        return False

    status_code = response["ResponseMetadata"]["HTTPStatusCode"]
    logger.info("Delete bucket status code: %s", status_code)
    return status_code == 200


def bucket_exists(aws_s3_client, bucket_name):
    logger.info("Checking if bucket '%s' exists", bucket_name)
    try:
        response = aws_s3_client.head_bucket(Bucket=bucket_name)
    except ClientError:
        logger.exception("Bucket '%s' does not exist or is inaccessible", bucket_name)
        return False

    status_code = response["ResponseMetadata"]["HTTPStatusCode"]
    logger.info("Head bucket status code: %s", status_code)
    return status_code == 200


def _validate_file_type(content: bytes, file_name: str) -> str:
    extension = Path(file_name).suffix.lower()
    if extension not in ALLOWED_EXTENSIONS:
        raise ValueError(f"Extension '{extension}' is not allowed")

    kind = filetype.guess(content)
    if kind is None:
        raise ValueError(f"Could not detect file type for '{file_name}'")

    detected_mime_type = kind.mime
    if detected_mime_type not in ALLOWED_MIME_TYPES:
        raise ValueError(
            "File type '%s' is not allowed. Allowed: .bmp .jpg .jpeg .png .webp .mp4"
            % detected_mime_type
        )

    if extension in {".jpg", ".jpeg"} and detected_mime_type != "image/jpeg":
        raise ValueError("JPEG file extension does not match detected MIME type")
    if extension == ".bmp" and detected_mime_type != "image/bmp":
        raise ValueError("BMP file extension does not match detected MIME type")
    if extension == ".png" and detected_mime_type != "image/png":
        raise ValueError("PNG file extension does not match detected MIME type")
    if extension == ".webp" and detected_mime_type != "image/webp":
        raise ValueError("WEBP file extension does not match detected MIME type")
    if extension == ".mp4" and detected_mime_type != "video/mp4":
        raise ValueError("MP4 file extension does not match detected MIME type")

    return detected_mime_type


def download_file_and_upload_to_s3(
    aws_s3_client,
    bucket_name,
    url,
    file_name,
    keep_local=False,
):
    logger.info("Downloading file from URL: %s", url)
    with urlopen(url) as response:
        content = response.read()

    logger.info("Validating file type for '%s'", file_name)
    try:
        detected_mime_type = _validate_file_type(content=content, file_name=file_name)
    except ValueError:
        logger.exception("File type validation failed for '%s'", file_name)
        raise

    logger.info("Uploading '%s' to bucket '%s'", file_name, bucket_name)
    try:
        aws_s3_client.upload_fileobj(
            Fileobj=io.BytesIO(content),
            Bucket=bucket_name,
            Key=file_name,
            ExtraArgs={"ContentType": detected_mime_type},
        )
    except ClientError:
        logger.exception("Upload failed for '%s'", file_name)
        raise

    if keep_local:
        logger.info("Saving local copy of '%s'", file_name)
        with open(file_name, mode="wb") as file:
            file.write(content)

    region = _get_env("AWS_REGION_NAME") or "us-west-2"
    object_url = f"https://s3-{region}.amazonaws.com/{bucket_name}/{file_name}"
    logger.info("File uploaded successfully: %s", object_url)
    return object_url


def set_object_access_policy(aws_s3_client, bucket_name, file_name):
    logger.info("Setting object ACL public-read for '%s/%s'", bucket_name, file_name)
    try:
        response = aws_s3_client.put_object_acl(
            ACL="public-read",
            Bucket=bucket_name,
            Key=file_name,
        )
    except ClientError:
        logger.exception("Failed to set object ACL")
        return False

    status_code = response["ResponseMetadata"]["HTTPStatusCode"]
    logger.info("Put object ACL status code: %s", status_code)
    return status_code == 200


def generate_public_read_policy(bucket_name):
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


def create_bucket_policy(aws_s3_client, bucket_name):
    logger.info("Creating bucket policy for '%s'", bucket_name)
    try:
        aws_s3_client.delete_public_access_block(Bucket=bucket_name)
        aws_s3_client.put_bucket_policy(
            Bucket=bucket_name,
            Policy=generate_public_read_policy(bucket_name),
        )
        logger.info("Bucket policy created successfully for '%s'", bucket_name)
        return True
    except ClientError:
        logger.exception("Failed to create bucket policy for '%s'", bucket_name)
        return False


def read_bucket_policy(aws_s3_client, bucket_name):
    logger.info("Reading bucket policy for '%s'", bucket_name)
    try:
        policy = aws_s3_client.get_bucket_policy(Bucket=bucket_name)
        policy_str = policy["Policy"]
        logger.info("Bucket policy: %s", policy_str)
        return policy_str
    except ClientError:
        logger.exception("Failed to read bucket policy for '%s'", bucket_name)
        return False


@click.group()
@click.option("--log-level", default="INFO", show_default=True)
def cli(log_level: str):
    logging.basicConfig(
        level=getattr(logging, log_level.upper(), logging.INFO),
        format="%(asctime)s | %(levelname)s | %(name)s | %(message)s",
    )


@cli.command(name="init_client")
def init_client_command():
    init_client()


@cli.command(name="list_buckets")
def list_buckets_command():
    client = init_client()
    response = list_buckets(client)
    if response and response.get("Buckets"):
        for bucket in response["Buckets"]:
            logger.info("Bucket: %s", bucket["Name"])


@cli.command(name="create_bucket")
@click.option("--bucket-name", required=True)
@click.option("--region", default="us-west-2", show_default=True)
def create_bucket_command(bucket_name: str, region: str):
    client = init_client()
    status = create_bucket(client, bucket_name, region)
    logger.info("create_bucket result: %s", status)


@cli.command(name="delete_bucket")
@click.option("--bucket-name", required=True)
def delete_bucket_command(bucket_name: str):
    client = init_client()
    status = delete_bucket(client, bucket_name)
    logger.info("delete_bucket result: %s", status)


@cli.command(name="bucket_exists")
@click.option("--bucket-name", required=True)
def bucket_exists_command(bucket_name: str):
    client = init_client()
    status = bucket_exists(client, bucket_name)
    logger.info("bucket_exists result: %s", status)


@cli.command(name="download_file_and_upload_to_s3")
@click.option("--bucket-name", required=True)
@click.option("--url", required=True)
@click.option("--file-name", required=True)
@click.option("--keep-local", is_flag=True, default=False)
def download_file_and_upload_to_s3_command(
    bucket_name: str,
    url: str,
    file_name: str,
    keep_local: bool,
):
    client = init_client()
    object_url = download_file_and_upload_to_s3(
        client,
        bucket_name,
        url,
        file_name,
        keep_local,
    )
    logger.info("Object URL: %s", object_url)


@cli.command(name="set_object_access_policy")
@click.option("--bucket-name", required=True)
@click.option("--file-name", required=True)
def set_object_access_policy_command(bucket_name: str, file_name: str):
    client = init_client()
    status = set_object_access_policy(client, bucket_name, file_name)
    logger.info("set_object_access_policy result: %s", status)


@cli.command(name="generate_public_read_policy")
@click.option("--bucket-name", required=True)
def generate_public_read_policy_command(bucket_name: str):
    policy = generate_public_read_policy(bucket_name)
    logger.info("Generated policy: %s", policy)


@cli.command(name="create_bucket_policy")
@click.option("--bucket-name", required=True)
def create_bucket_policy_command(bucket_name: str):
    client = init_client()
    status = create_bucket_policy(client, bucket_name)
    logger.info("create_bucket_policy result: %s", status)


@cli.command(name="read_bucket_policy")
@click.option("--bucket-name", required=True)
def read_bucket_policy_command(bucket_name: str):
    client = init_client()
    policy = read_bucket_policy(client, bucket_name)
    logger.info("read_bucket_policy result: %s", policy)


if __name__ == "__main__":
    cli()
