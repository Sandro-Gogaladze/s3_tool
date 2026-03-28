from pathlib import Path

import filetype

ALLOWED_EXTENSIONS = {".bmp", ".jpg", ".jpeg", ".png", ".webp", ".mp4"}
ALLOWED_MIME_TYPES = {
    "image/bmp",
    "image/jpeg",
    "image/png",
    "image/webp",
    "video/mp4",
}


def _validate_file_type(content: bytes, file_name: str) -> str:
    """Validate a file's real type using magic bytes, not just its extension.

    Returns the detected MIME type on success, raises ValueError on any mismatch.
    `content` only needs to be the first 261 bytes — that is enough for filetype
    to identify all formats in ALLOWED_MIME_TYPES.
    """
    extension = Path(file_name).suffix.lower()
    if extension not in ALLOWED_EXTENSIONS:
        raise ValueError(f"Extension '{extension}' is not allowed")

    # filetype.guess inspects magic bytes (the first few bytes of the file),
    # not the file name, so renaming a PNG to .jpg will still be caught below.
    kind = filetype.guess(content)
    if kind is None:
        raise ValueError(f"Could not detect file type for '{file_name}'")

    detected_mime_type = kind.mime
    if detected_mime_type not in ALLOWED_MIME_TYPES:
        raise ValueError(
            "File type '%s' is not allowed. Allowed: .bmp .jpg .jpeg .png .webp .mp4"
            % detected_mime_type
        )

    # Cross-check: the extension must agree with what the magic bytes say.
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
