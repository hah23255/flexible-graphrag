# Per-source helpers for FlexibleDataSource / FlexibleMapView.
#
# Two APIs live here:
#   * eager iterators ``iter_<source>(config)`` (Phase 1 fallback path)
#   * Phase 2 lazy API ``list_metadata(config)`` + ``build_source(config)`` +
#     ``download_one(source, key)`` per module, all delegating to ``_lazy``.
#
# The map-view uses the shared ``_lazy`` helpers directly (keyed by
# source_type) so it never has to import each module individually.

from ._lazy import (  # noqa: F401
    DETECTOR_BACKED,
    FileRecord,
    build_source,
    download_one,
    list_metadata,
    map_record,
)

__all__ = [
    "DETECTOR_BACKED",
    "FileRecord",
    "build_source",
    "download_one",
    "list_metadata",
    "map_record",
]
