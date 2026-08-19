"""CocoIndex-native Azure Blob source connector."""

from __future__ import annotations

import logging
import os
from dataclasses import dataclass
from typing import Any, Dict, Optional

from cocoindex_integration.connectors.cocoindex.base import CocoSource

logger = logging.getLogger(__name__)


def _azure_blob_native_available() -> bool:
    try:
        from cocoindex.connectors import azure_blob  # noqa: F401, PLC0415
        from azure.storage.blob.aio import ContainerClient  # noqa: F401, PLC0415
        return True
    except Exception:  # noqa: BLE001
        return False


@dataclass
class CocoAzureBlob(CocoSource):
    """Descriptor for the native CocoIndex Azure Blob source (read-only)."""

    name = "azure_blob"
    can_read = True
    can_write = False

    container: str = ""
    prefix: str = ""
    account_url: str = ""


def _resolve_azure_blob_config(cfg: Dict[str, Any]) -> Dict[str, Any]:
    """Merge pipeline cfg with ``AZURE_BLOB_*`` env vars."""
    merged: Dict[str, Any] = dict(cfg or {})
    for _k, _v in os.environ.items():
        if _k.startswith("AZURE_BLOB_"):
            merged[_k[12:].lower()] = _v
    merged.setdefault("container_name", os.getenv("AZURE_BLOB_CONTAINER_NAME", ""))
    merged.setdefault("container", merged.get("container_name", ""))
    merged.setdefault("prefix", os.getenv("AZURE_BLOB_PREFIX", ""))
    merged.setdefault("account_url", os.getenv("AZURE_BLOB_ACCOUNT_URL", ""))
    merged.setdefault("account_name", os.getenv("AZURE_BLOB_ACCOUNT_NAME", ""))
    merged.setdefault("account_key", os.getenv("AZURE_BLOB_ACCOUNT_KEY", ""))
    merged.setdefault("sas_token", os.getenv("AZURE_BLOB_SAS_TOKEN", ""))
    return merged


def build_azure_blob(cfg: Dict[str, Any]) -> Optional[CocoAzureBlob]:
    """Build a :class:`CocoAzureBlob` descriptor."""
    resolved = _resolve_azure_blob_config(cfg)
    container = str(resolved.get("container_name", resolved.get("container", "")))
    account_url = str(resolved.get("account_url", ""))
    account_name = str(resolved.get("account_name", ""))
    if not account_url and account_name:
        account_url = f"https://{account_name}.blob.core.windows.net"
    src = CocoAzureBlob(
        container=container,
        prefix=str(resolved.get("prefix", "")),
        account_url=account_url,
    )
    src.native_available = (
        _azure_blob_native_available()
        and bool(container)
        and bool(account_url)
    )
    if src.native_available:
        logger.info(
            "[coco] azure_blob: native CocoIndex Azure Blob connector ready "
            "(container=%s prefix=%r url=%s)",
            src.container, src.prefix or "", src.account_url,
        )
    elif _azure_blob_native_available() and (not container or not account_url):
        logger.warning(
            "[coco] azure_blob: container/account_url not set — "
            "using FlexibleDataSource('azure_blob')"
        )
    else:
        logger.warning(
            "[coco] azure_blob: native connector unavailable — "
            "using FlexibleDataSource('azure_blob')"
        )
    return src
