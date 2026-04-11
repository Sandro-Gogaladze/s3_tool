import io
import logging
import mimetypes
import os
from pathlib import Path
from urllib.request import urlopen

from botocore.exceptions import ClientError

from .client import _get_env
from .validation import _validate_file_type

logger = logging.getLogger(__name__)

# S3 multipart upload rules:
#   - Each part (except the last) must be at least 5 MB.
#   - The last part can be smaller.
#   - Maximum 10,000 parts per upload.
CHUNK_SIZE = 5 * 1024 * 1024  # 5 MB — minimum part size enforced by S3

# Files at or above this threshold should use upload_large_file (multipart).
# put_object loads the entire body into memory, so it is only practical for small files.
MULTIPART_THRESHOLD = 5 * 1024 * 1024  # 5 MB


def upload_small_file(
    aws_s3_client,
    bucket_name: str,
    file_path: str,
    s3_key: str | None = None,
    validate_mime: bool = False,
) -> bool:
    """Upload a file using put_object (single HTTP request, whole file in memory).

    Suitable for files under MULTIPART_THRESHOLD. For larger files use
    upload_large_file instead.
    """
    if not os.path.isfile(file_path):
        raise FileNotFoundError(f"File not found: '{file_path}'")

    if s3_key is None:
        s3_key = Path(file_path).name

    with open(file_path, "rb") as f:
        content = f.read()

    # Warn if the caller chose the wrong upload function for the file size.
    file_size = len(content)
    if file_size > MULTIPART_THRESHOLD:
        logger.info(
            "File '%s' is %s bytes (> %s MB threshold); consider upload_large_file",
            file_path, file_size, MULTIPART_THRESHOLD // (1024 * 1024),
        )

    content_type = "application/octet-stream"
    if validate_mime:
        try:
            content_type = _validate_file_type(content, s3_key)
            logger.info("MIME validated: %s", content_type)
        except ValueError:
            logger.exception("MIME validation failed for '%s'", file_path)
            raise

    logger.info("Uploading small file '%s' to s3://%s/%s", file_path, bucket_name, s3_key)
    try:
        aws_s3_client.put_object(Bucket=bucket_name, Key=s3_key, Body=content, ContentType=content_type)
    except ClientError:
        logger.exception("put_object failed for '%s'", file_path)
        raise
    logger.info("Small upload complete: s3://%s/%s", bucket_name, s3_key)
    return True


def upload_large_file(
    aws_s3_client,
    bucket_name: str,
    file_path: str,
    s3_key: str | None = None,
    validate_mime: bool = False,
) -> bool:
    """Upload a file using the S3 multipart upload API.

    Three-step flow required by S3:
      1. create_multipart_upload  — AWS returns an UploadId that groups all parts.
      2. upload_part in a loop    — each CHUNK_SIZE chunk gets an ETag from AWS.
      3. complete_multipart_upload — AWS assembles all parts into the final object.

    If anything fails after the upload is initiated, abort_multipart_upload is
    called so AWS discards the orphaned parts and stops billing for them.
    """
    if not os.path.isfile(file_path):
        raise FileNotFoundError(f"File not found: '{file_path}'")

    if s3_key is None:
        s3_key = Path(file_path).name

    content_type = "application/octet-stream"
    if validate_mime:
        # Read only the first 261 bytes — enough for filetype magic-byte detection.
        # Avoids loading the whole (potentially large) file just for validation.
        with open(file_path, "rb") as f:
            sample = f.read(261)
        try:
            content_type = _validate_file_type(sample, s3_key)
            logger.info("MIME validated: %s", content_type)
        except ValueError:
            logger.exception("MIME validation failed for '%s'", file_path)
            raise

    file_size = os.path.getsize(file_path)
    logger.info(
        "Starting multipart upload: '%s' (%s bytes) -> s3://%s/%s",
        file_path, file_size, bucket_name, s3_key,
    )

    # Step 1: initiate — must succeed before we open the file or start looping.
    try:
        mpu = aws_s3_client.create_multipart_upload(
            Bucket=bucket_name, Key=s3_key, ContentType=content_type
        )
    except ClientError:
        logger.exception("create_multipart_upload failed for '%s'", file_path)
        raise
    upload_id = mpu["UploadId"]
    logger.info("Multipart upload initiated, UploadId: %s", upload_id)

    parts = []

    try:
        # Step 2: stream the file in CHUNK_SIZE pieces. Each part must be
        # at least 5 MB (S3 requirement), except the final part.
        with open(file_path, "rb") as f:
            part_number = 1
            while True:
                chunk = f.read(CHUNK_SIZE)
                if not chunk:
                    break
                logger.info("Uploading part %s (%s bytes)", part_number, len(chunk))
                response = aws_s3_client.upload_part(
                    Bucket=bucket_name,
                    Key=s3_key,
                    UploadId=upload_id,
                    PartNumber=part_number,
                    Body=chunk,
                )
                # AWS returns an ETag for each part; all ETags are required
                # in the complete call so AWS can verify the assembly.
                parts.append({"PartNumber": part_number, "ETag": response["ETag"]})
                part_number += 1

        # Step 3: tell AWS to assemble all parts into the final object.
        aws_s3_client.complete_multipart_upload(
            Bucket=bucket_name,
            Key=s3_key,
            UploadId=upload_id,
            MultipartUpload={"Parts": parts},
        )
        logger.info("Multipart upload complete: s3://%s/%s", bucket_name, s3_key)
        return True

    except Exception:
        # Always abort on failure — otherwise AWS stores orphaned parts and
        # continues billing for their storage until they expire.
        logger.exception("Multipart upload failed, aborting UploadId: %s", upload_id)
        aws_s3_client.abort_multipart_upload(Bucket=bucket_name, Key=s3_key, UploadId=upload_id)
        logger.info("Multipart upload aborted")
        raise


def upload_directory(aws_s3_client, bucket_name: str, source_dir: str) -> int:
    """Recursively upload all files from *source_dir* to *bucket_name*.

    Preserves folder structure relative to *source_dir* and sets the
    Content-Type for each file using mimetypes.guess_type.

    Returns the number of files uploaded.
    """
    source_path = Path(source_dir).resolve()
    if not source_path.is_dir():
        raise NotADirectoryError(f"Source directory not found: '{source_dir}'")

    uploaded = 0
    for file_path in sorted(source_path.rglob("*")):
        if not file_path.is_file():
            continue

        # Skip hidden files and anything inside hidden directories (e.g. .git/)
        relative = file_path.relative_to(source_path)
        if any(part.startswith(".") for part in relative.parts):
            continue

        s3_key = str(file_path.relative_to(source_path))
        content_type, _ = mimetypes.guess_type(str(file_path))
        if content_type is None:
            content_type = "application/octet-stream"

        file_size = file_path.stat().st_size
        logger.info("Uploading '%s' -> s3://%s/%s (%s)", file_path, bucket_name, s3_key, content_type)

        try:
            if file_size <= MULTIPART_THRESHOLD:
                with open(file_path, "rb") as f:
                    aws_s3_client.put_object(
                        Bucket=bucket_name, Key=s3_key, Body=f.read(), ContentType=content_type,
                    )
            else:
                mpu = aws_s3_client.create_multipart_upload(
                    Bucket=bucket_name, Key=s3_key, ContentType=content_type,
                )
                upload_id = mpu["UploadId"]
                parts = []
                try:
                    with open(file_path, "rb") as f:
                        part_number = 1
                        while True:
                            chunk = f.read(CHUNK_SIZE)
                            if not chunk:
                                break
                            response = aws_s3_client.upload_part(
                                Bucket=bucket_name, Key=s3_key,
                                UploadId=upload_id, PartNumber=part_number, Body=chunk,
                            )
                            parts.append({"PartNumber": part_number, "ETag": response["ETag"]})
                            part_number += 1
                    aws_s3_client.complete_multipart_upload(
                        Bucket=bucket_name, Key=s3_key, UploadId=upload_id,
                        MultipartUpload={"Parts": parts},
                    )
                except Exception:
                    logger.exception("Multipart upload failed for '%s', aborting", s3_key)
                    aws_s3_client.abort_multipart_upload(Bucket=bucket_name, Key=s3_key, UploadId=upload_id)
                    raise
        except ClientError:
            logger.exception("Failed to upload '%s'", file_path)
            raise

        uploaded += 1

    logger.info("Uploaded %s file(s) from '%s' to s3://%s", uploaded, source_dir, bucket_name)
    return uploaded


def download_file_and_upload_to_s3(
    aws_s3_client,
    bucket_name: str,
    url: str,
    file_name: str,
    keep_local: bool = False,
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
