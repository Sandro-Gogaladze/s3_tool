import logging

import click

from .client import init_client
from .versioning import (
    delete_versions_older_than_six_months,
    get_versioning_status,
    list_object_versions,
    restore_previous_version,
)

logger = logging.getLogger(__name__)


def register_versioning_commands(cli):
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

    @cli.command(name="delete_old_versions")
    @click.option("--bucket-name", required=True, help="Bucket that contains the objects.")
    @click.option("--key", required=True, multiple=True, help="S3 object key to check. Can be passed multiple times.")
    def delete_old_versions_command(bucket_name: str, key: tuple[str, ...]):
        """Delete versions older than six months for the given object keys."""
        client = init_client()
        counts = delete_versions_older_than_six_months(client, bucket_name, key)
        for object_key, count in counts.items():
            logger.info("%s - deleted %s old version(s)", object_key, count)
