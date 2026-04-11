"""CLI entry point for s3-tool.

All business logic lives in the sibling modules:
  client.py     — AWS client initialisation
  validation.py — MIME/extension validation
  buckets.py    — bucket CRUD and policies
  uploads.py    — small/large file upload and URL-download-then-upload
  lifecycle.py  — lifecycle policy management
  quotes.py     — inspirational-quote fetching
"""

import json
import logging
from datetime import datetime, timezone

import click

from .buckets import (
    bucket_exists,
    configure_website,
    create_bucket,
    create_bucket_policy,
    delete_bucket,
    delete_object,
    disable_public_access_block,
    generate_public_read_policy,
    get_versioning_status,
    list_buckets,
    list_object_versions,
    organize_by_extension,
    read_bucket_policy,
    restore_previous_version,
    set_object_access_policy,
)
from .client import init_client
from .lifecycle import read_lifecycle_policy, set_lifecycle_policy
from .quotes import get_quote, print_quote
from .uploads import download_file_and_upload_to_s3, upload_directory, upload_large_file, upload_small_file

logger = logging.getLogger(__name__)


@click.group()
@click.option("--log-level", default="INFO", show_default=True)
def cli(log_level: str):
    logging.basicConfig(
        level=getattr(logging, log_level.upper(), logging.INFO),
        format="%(asctime)s | %(levelname)s | %(name)s | %(message)s",
    )


# ── Bucket commands ───────────────────────────────────────────────────────────

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
def download_file_and_upload_to_s3_command(bucket_name: str, url: str, file_name: str, keep_local: bool):
    client = init_client()
    object_url = download_file_and_upload_to_s3(client, bucket_name, url, file_name, keep_local)
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


@cli.command(name="delete_object")
@click.option("--bucket-name", required=True, help="Bucket that contains the object.")
@click.option("--key", required=True, help="S3 object key (file name) to delete.")
@click.option("-del", "confirm", is_flag=True, default=False, help="Required flag to confirm deletion.")
def delete_object_command(bucket_name: str, key: str, confirm: bool):
    """Delete a specific object from a bucket. Must pass -del to confirm."""
    if not confirm:
        logger.info("Deletion aborted: pass -del to confirm you want to delete '%s'", key)
        return
    client = init_client()
    status = delete_object(client, bucket_name, key)
    logger.info("delete_object result: %s", status)


# ── Organize command ──────────────────────────────────────────────────────────

@cli.command(name="organize")
@click.option("--bucket-name", required=True, help="Bucket to organize.")
@click.option("--organize", "confirm", is_flag=True, default=False, help="Move objects into extension-named folders.")
def organize_command(bucket_name: str, confirm: bool):
    """Group all root-level objects into folders named after their extension."""
    if not confirm:
        logger.info("Pass --organize to run organization on bucket '%s'", bucket_name)
        return
    client = init_client()
    counts = organize_by_extension(client, bucket_name)
    for ext, count in sorted(counts.items()):
        logger.info("%s - %s", ext, count)


# ── Versioning commands ───────────────────────────────────────────────────────

@cli.command(name="versioning_status")
@click.option("--bucket-name", required=True, help="Bucket to check.")
@click.option("--versioning", "check", is_flag=True, default=False, help="Show versioning status.")
def versioning_status_command(bucket_name: str, check: bool):
    """Check whether versioning is enabled on a bucket."""
    if not check:
        logger.info("Pass --versioning to check versioning status")
        return
    client = init_client()
    status = get_versioning_status(client, bucket_name)
    logger.info("versioning_status result: %s", status)


@cli.command(name="object_versions")
@click.option("--bucket-name", required=True, help="Bucket that contains the object.")
@click.option("--key", required=True, help="S3 object key (file name).")
@click.option("--versions", "show", is_flag=True, default=False, help="List all versions of the object.")
def object_versions_command(bucket_name: str, key: str, show: bool):
    """Show version count and creation dates for a specific object."""
    if not show:
        logger.info("Pass --versions to list versions of '%s'", key)
        return
    client = init_client()
    versions = list_object_versions(client, bucket_name, key)
    logger.info("Total versions for '%s': %s", key, len(versions))


@cli.command(name="restore_version")
@click.option("--bucket-name", required=True, help="Bucket that contains the object.")
@click.option("--key", required=True, help="S3 object key (file name).")
@click.option("--restore", "confirm", is_flag=True, default=False, help="Restore the previous version as the new latest.")
def restore_version_command(bucket_name: str, key: str, confirm: bool):
    """Upload the previous version of an object as the new latest version."""
    if not confirm:
        logger.info("Pass --restore to restore previous version of '%s'", key)
        return
    client = init_client()
    status = restore_previous_version(client, bucket_name, key)
    logger.info("restore_version result: %s", status)


# ── Upload commands ───────────────────────────────────────────────────────────

@cli.command(name="upload_small")
@click.option("--bucket-name", required=True, help="Destination S3 bucket.")
@click.option("--file-path", required=True, help="Local path to the file.")
@click.option("--s3-key", default=None, help="S3 object key. Defaults to filename.")
@click.option("--validate-mime", is_flag=True, default=False, help="Validate real MIME type before uploading.")
def upload_small_command(bucket_name: str, file_path: str, s3_key: str, validate_mime: bool):
    """Upload a small file (<= 5 MB) using put_object (single HTTP request)."""
    client = init_client()
    status = upload_small_file(client, bucket_name, file_path, s3_key, validate_mime)
    logger.info("upload_small result: %s", status)


@cli.command(name="upload_large")
@click.option("--bucket-name", required=True, help="Destination S3 bucket.")
@click.option("--file-path", required=True, help="Local path to the file.")
@click.option("--s3-key", default=None, help="S3 object key. Defaults to filename.")
@click.option("--validate-mime", is_flag=True, default=False, help="Validate real MIME type before uploading.")
def upload_large_command(bucket_name: str, file_path: str, s3_key: str, validate_mime: bool):
    """Upload a large file (> 5 MB) using multipart upload (chunked into 5 MB parts)."""
    client = init_client()
    status = upload_large_file(client, bucket_name, file_path, s3_key, validate_mime)
    logger.info("upload_large result: %s", status)


# ── Lifecycle commands ────────────────────────────────────────────────────────

@cli.command(name="set_lifecycle")
@click.option("--bucket-name", required=True, help="Bucket to apply the lifecycle policy to.")
@click.option("--days", default=120, show_default=True, help="Delete objects after this many days.")
def set_lifecycle_command(bucket_name: str, days: int):
    """Set a lifecycle policy to auto-delete all objects after N days (default: 120)."""
    client = init_client()
    status = set_lifecycle_policy(client, bucket_name, days)
    logger.info("set_lifecycle result: %s", status)


@cli.command(name="read_lifecycle")
@click.option("--bucket-name", required=True, help="Bucket whose lifecycle policy to read.")
def read_lifecycle_command(bucket_name: str):
    """Read and display the current lifecycle policy of a bucket."""
    client = init_client()
    rules = read_lifecycle_policy(client, bucket_name)
    if rules:
        for rule in rules:
            logger.info("Rule: %s", rule)
    else:
        logger.info("No lifecycle policy found on '%s'", bucket_name)


# ── Host command ─────────────────────────────────────────────────────────────

@cli.command(name="host")
@click.argument("bucket_name")
@click.option("--source", required=True, help="Local folder to upload.")
@click.option("--region", default="us-east-1", show_default=True, help="AWS region for the bucket.")
def host_command(bucket_name: str, source: str, region: str):
    """Create (or reuse) a bucket, upload a folder, and enable static website hosting."""
    client = init_client()

    if not bucket_exists(client, bucket_name):
        logger.info("Bucket '%s' does not exist, creating it", bucket_name)
        create_bucket(client, bucket_name, region)
    else:
        logger.info("Bucket '%s' already exists, reusing it", bucket_name)

    disable_public_access_block(client, bucket_name)
    upload_directory(client, bucket_name, source)
    configure_website(client, bucket_name)
    create_bucket_policy(client, bucket_name)

    website_url = f"http://{bucket_name}.s3-website-{region}.amazonaws.com"
    logger.info("Static website URL: %s", website_url)


# ── Inspire command ──────────────────────────────────────────────────────────

@cli.command(name="inspire")
@click.argument("bucket_name", required=False, default=None)
@click.option("--inspire", "author", default=None, help="Author name to filter by. Omit value for a random quote.")
@click.option("--save", is_flag=True, default=False, help="Save the quote as JSON to the given S3 bucket.")
def inspire_command(bucket_name: str | None, author: str | None, save: bool):
    """Fetch an inspirational quote, optionally save it to S3."""
    data = get_quote(author)
    if not data:
        logger.error("Could not fetch a quote")
        return

    print_quote(data)

    if save:
        if not bucket_name:
            logger.error("--save requires a bucket name: s3-tool inspire <bucket> --save")
            return
        client = init_client()
        timestamp = datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S")
        s3_key = f"quote_{timestamp}.json"
        body = json.dumps(data, indent=2).encode()
        logger.info("Saving quote to s3://%s/%s", bucket_name, s3_key)
        client.put_object(Bucket=bucket_name, Key=s3_key, Body=body, ContentType="application/json")
        logger.info("Quote saved to s3://%s/%s", bucket_name, s3_key)


if __name__ == "__main__":
    cli()
