"""CocoIndex-native Amazon S3 source connector."""

from __future__ import annotations

import logging
import os
from dataclasses import dataclass
from typing import Any, Dict, Optional

from cocoindex_integration.connectors.cocoindex.base import CocoSource

logger = logging.getLogger(__name__)


def _s3_native_available() -> bool:
    """True when CocoIndex's native S3 connector and aiobotocore are importable."""
    try:
        from cocoindex.connectors import amazon_s3  # noqa: F401, PLC0415
        import aiobotocore.session  # noqa: F401, PLC0415
        return True
    except Exception:  # noqa: BLE001
        return False


@dataclass
class CocoAmazonS3(CocoSource):
    """Descriptor for the native CocoIndex Amazon S3 source (read-only)."""
    name = "amazon_s3"
    can_read = True
    can_write = False

    bucket: str = ""
    prefix: str = ""
    region: str = "us-east-1"
    endpoint_url: str = ""


def _resolve_s3_config(cfg: Dict[str, Any]) -> Dict[str, Any]:
    """Merge pipeline cfg with ``S3_*`` env vars and ``S3_CONFIG`` JSON blob.

    Priority (highest first):
    1. ``S3_CONFIG`` JSON blob keys  (e.g. ``{"bucket": "my-bucket", ...}``)
    2. Individual ``S3_*`` env vars  (e.g. ``S3_BUCKET``, ``S3_BUCKET_NAME``)
    3. Standard AWS env vars         (e.g. ``AWS_DEFAULT_REGION``)
    4. Passed-in ``cfg`` dict
    """
    import json as _json  # noqa: PLC0415

    merged: Dict[str, Any] = dict(cfg)

    # Individual S3_* env vars (lower priority than JSON blob)
    for _k, _v in os.environ.items():
        if _k.startswith("S3_") and _k != "S3_CONFIG":
            merged[_k[3:].lower()] = _v

    # S3_CONFIG JSON blob overrides everything above (same precedence as config.py)
    _s3_cfg_raw = os.getenv("S3_CONFIG", "")
    if _s3_cfg_raw:
        try:
            _s3_blob: Dict[str, Any] = _json.loads(_s3_cfg_raw)
            merged.update(_s3_blob)
        except Exception:
            pass

    # Normalise alternate key names so downstream always sees "bucket" / "region"
    if not merged.get("bucket"):
        merged["bucket"] = merged.get("bucket_name", "")
    if not merged.get("region"):
        merged["region"] = merged.get("region_name",
                           os.getenv("AWS_REGION",
                           os.getenv("AWS_DEFAULT_REGION", "us-east-1")))

    merged.setdefault("prefix", "")
    if os.getenv("S3_ENDPOINT_URL"):
        merged["endpoint_url"] = os.getenv("S3_ENDPOINT_URL", "")
    return merged


def build_amazon_s3(cfg: Dict[str, Any]) -> Optional[CocoAmazonS3]:
    """Build a :class:`CocoAmazonS3` descriptor."""
    resolved = _resolve_s3_config(cfg or {})
    src = CocoAmazonS3(
        bucket=str(resolved.get("bucket", "")),
        prefix=str(resolved.get("prefix", "")),
        region=str(resolved.get("region", "us-east-1")),
        endpoint_url=str(resolved.get("endpoint_url", "")),
    )
    src.native_available = _s3_native_available() and bool(src.bucket)
    if src.native_available:
        logger.info(
            "[coco] amazon_s3: native CocoIndex S3 connector ready "
            "(bucket=%s prefix=%r)", src.bucket, src.prefix or "",
        )
    elif _s3_native_available() and not src.bucket:
        logger.warning(
            "[coco] amazon_s3: S3_BUCKET not set — using FlexibleDataSource('s3')"
        )
    else:
        logger.warning(
            "[coco] amazon_s3: native connector unavailable — "
            "using FlexibleDataSource('s3')"
        )
    return src
