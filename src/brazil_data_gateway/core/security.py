"""Security controls for untrusted external government payloads (CWE-1236, Zip-Bomb guard, MCP wrapping)."""

import json
import re
import zipfile
from pathlib import Path
from typing import Any

# Magic byte signatures for expected file formats
_MAGIC_BYTES: dict[str, list[bytes]] = {
    "zip": [b"PK\x03\x04", b"PK\x05\x06", b"PK\x07\x08"],
    "parquet": [b"PAR1"],
    "sqlite": [b"SQLite format 3\x00"],
    "gzip": [b"\x1f\x8b"],
}

# Regex to detect valid numeric strings (including negative numbers and Brazilian decimals)
_NUMERIC_PATTERN = re.compile(r"^-?\d+(?:[.,]\d+)?(?:[eE][+-]?\d+)?$")
_DANGEROUS_PREFIXES = ("=", "+", "-", "@", "\t", "\r")


def verify_magic_bytes(data: bytes, expected: str) -> bool:
    """Validate that raw file content starts with expected magic signature bytes."""
    normalized_key = expected.lower().strip()
    signatures = _MAGIC_BYTES.get(normalized_key)
    if not signatures:
        raise ValueError(f"Unsupported format signature check: {expected}")
    return any(data.startswith(sig) for sig in signatures)


def safe_extract_zip(
    zip_path: Path,
    dest_dir: Path,
    max_ratio: float = 100.0,
    max_uncompressed_bytes: int = 2 * 1024**3,
) -> list[Path]:
    """Safely extracts a ZIP archive, defending against Zip Slip (CWE-22) and Zip Bombs (CWE-409)."""
    dest_resolved = dest_dir.resolve()
    dest_resolved.mkdir(parents=True, exist_ok=True)

    extracted_paths: list[Path] = []
    total_uncompressed = 0

    with zipfile.ZipFile(zip_path, "r") as zf:
        for member in zf.infolist():
            # Defense against Zip Slip
            target_path = (dest_resolved / member.filename).resolve()
            try:
                target_path.relative_to(dest_resolved)
            except ValueError as err:
                raise ValueError(
                    f"Zip slip detected: member '{member.filename}' escapes destination directory."
                ) from err

            # Defense against Zip Bomb: Ratio check
            compress_size = max(member.compress_size, 1)
            ratio = member.file_size / compress_size
            if member.file_size > 1024 and ratio > max_ratio:
                raise ValueError(
                    f"Zip bomb detected: compression ratio {ratio:.1f} exceeds ceiling {max_ratio}."
                )

            # Defense against Zip Bomb: Cumulative size check
            total_uncompressed += member.file_size
            if total_uncompressed > max_uncompressed_bytes:
                raise ValueError(
                    f"Zip bomb detected: uncompressed size exceeds {max_uncompressed_bytes} bytes."
                )

        # Extract file by file safely
        for member in zf.infolist():
            target_path = (dest_resolved / member.filename).resolve()
            if member.is_dir():
                target_path.mkdir(parents=True, exist_ok=True)
                continue

            target_path.parent.mkdir(parents=True, exist_ok=True)
            with zf.open(member) as source, open(target_path, "wb") as dest:
                bytes_written = 0
                while chunk := source.read(64 * 1024):
                    bytes_written += len(chunk)
                    dest.write(chunk)
            extracted_paths.append(target_path)

    return extracted_paths


def sanitize_csv_value(value: Any) -> Any:
    """Neutralize potential CSV formula injection (CWE-1236) while preserving legitimate negative numbers."""
    if not isinstance(value, str):
        return value

    stripped = value.strip()
    if not stripped:
        return value

    # Valid negative numbers like "-12.34" or "-123,45" are safe
    if stripped.startswith("-") and _NUMERIC_PATTERN.match(stripped):
        return value

    # Prefix dangerous characters if not already escaped
    if any(value.startswith(prefix) for prefix in _DANGEROUS_PREFIXES):
        return f"'{value}"

    return value


def wrap_untrusted_gov_data(payload: Any, source_id: str) -> str:
    """Wraps external, unverified government data in XML protection tags for safe LLM/MCP consumption."""
    if isinstance(payload, str):
        body = payload
    else:
        body = json.dumps(payload, ensure_ascii=False, indent=2, default=str)

    return f'<untrusted_gov_data source="{source_id}">\n{body}\n</untrusted_gov_data>'
