# S3 Tool CLI

Simple AWS S3 CLI built with Poetry, Click, boto3, python-dotenv, and filetype.

## Project structure

```text
s3_tool/
  pyproject.toml
  .env.example
  .gitignore
  README.md
  s3_tool/
    __init__.py
    main.py
```

## Setup

1. Install Poetry: https://python-poetry.org/docs/
2. Install dependencies:

```bash
poetry install
```

3. Create `.env` from `.env.example` and fill credentials.

## Environment variables

- `AWS_ACCESS_KEY_ID`
- `AWS_SECRET_ACCESS_KEY`
- `AWS_SESSION_TOKEN` (optional)
- `AWS_REGION_NAME`

## Run commands

Use either:

```bash
poetry run s3-tool --help
```

or

```bash
poetry run python -m s3_tool.main --help
```

### Available commands

- `init_client`
- `list_buckets`
- `create_bucket`
- `delete_bucket`
- `bucket_exists`
- `download_file_and_upload_to_s3`
- `set_object_access_policy`
- `generate_public_read_policy`
- `create_bucket_policy`
- `read_bucket_policy`

## Examples

```bash
poetry run s3-tool create_bucket --bucket-name my-demo-bucket --region us-west-2
poetry run s3-tool list_buckets
poetry run s3-tool bucket_exists --bucket-name my-demo-bucket
poetry run s3-tool download_file_and_upload_to_s3 \
  --bucket-name my-demo-bucket \
  --url "https://example.com/my-image.jpg" \
  --file-name "sample.jpg"
poetry run s3-tool set_object_access_policy --bucket-name my-demo-bucket --file-name sample.jpg
poetry run s3-tool create_bucket_policy --bucket-name my-demo-bucket
poetry run s3-tool read_bucket_policy --bucket-name my-demo-bucket
poetry run s3-tool delete_bucket --bucket-name my-demo-bucket
```

## MIME validation behavior

`download_file_and_upload_to_s3` validates file extension and **real MIME type** using `filetype`.
Allowed types:

- `.bmp` (`image/bmp`)
- `.jpg` / `.jpeg` (`image/jpeg`)
- `.png` (`image/png`)
- `.webp` (`image/webp`)
- `.mp4` (`video/mp4`)

No additional system library is required for MIME detection.

## Publish to Git

Initialize and push the project:

```bash
git init
git add .
git commit -m "Initial commit: AWS S3 CLI tool"
git branch -M main
git remote add origin <your-repo-url>
git push -u origin main
```

Before pushing, ensure `.env` is not staged:

```bash
git status
```
