"""CocoIndex-native message-stream connectors (placeholders).

CocoIndex v1 supports two message-stream systems as **both source and target**:

    Kafka   — Apache Kafka topics, at-least-once produce/consume semantics
    Iggy    — Apache Iggy topics as live streams or keyed maps

Stream connectors differ from document/vector connectors:

* **As source**: rows arrive from a topic as a live keyed map or raw event stream.
  The pipeline processes each event rather than scanning a file store.
* **As target**: target states are emitted as messages to a topic — useful for
  downstream consumers that react to document-processing events (e.g. search
  index updates, webhook notifications, analytics pipelines).

Neither is wired in this pipeline yet.  The entries exist so ``COCO_STREAM_REGISTRY``
is complete relative to https://cocoindex.io/docs/connectors/ and callers can
emit informative fallback messages.

Use cases in a GraphRAG pipeline
---------------------------------
* **Kafka/Iggy as source**: real-time document ingestion triggered by upstream
  producers (e.g. an S3 event notification fan-out or a change-data-capture stream).
* **Kafka/Iggy as target**: emit chunk-processed events to a downstream topic so
  other microservices react without polling (e.g. trigger a search re-index).

Implementation guide: https://cocoindex.io/docs/advanced/custom-target-connector/
"""

from __future__ import annotations

import logging
from typing import Any, Callable, Dict, Optional

from cocoindex_integration.connectors.cocoindex.base import CocoConnector

logger = logging.getLogger(__name__)


class CocoStream(CocoConnector):
    """Kind-base for CocoIndex-native message-stream connectors.

    Streams are dual-role: they can be a source (consume from topic) and/or
    a target (produce to topic) depending on configuration.
    """
    kind = "stream"
    can_read = True    # can consume from topic as a flow source
    can_write = True   # can produce to topic as a flow target


#: name → builder(cfg) -> Optional[CocoStream].
#: ``None`` = recognised name without a native implementation here.
COCO_STREAM_REGISTRY: Dict[
    str, Optional[Callable[[Dict[str, Any]], Optional[CocoStream]]]
] = {
    "kafka": None,   # Apache Kafka — at-least-once produce/consume
    "iggy": None,    # Apache Iggy  — topics as live streams or keyed maps
}

#: Stream names that have a CocoIndex stream descriptor.
COCO_STREAMS = frozenset(COCO_STREAM_REGISTRY)


def coco_stream(
    stream_name: str, cfg: Dict[str, Any]
) -> Optional[CocoStream]:
    """Return a CocoStream connector (always None — placeholders only).

    Returns ``None`` for both currently registered streams until native
    adapters are implemented.  *cfg* is reserved for future builders.
    """
    name_lower = stream_name.lower()
    if name_lower not in COCO_STREAM_REGISTRY:
        return None
    builder = COCO_STREAM_REGISTRY[name_lower]
    if builder is None:
        logger.warning(
            "[coco] %s: stream connector not yet implemented — "
            "see https://cocoindex.io/docs/connectors/%s/ for the native API",
            stream_name, name_lower,
        )
        return None
    return builder(cfg)


__all__ = [
    "CocoStream",
    "COCO_STREAM_REGISTRY",
    "COCO_STREAMS",
    "coco_stream",
]
