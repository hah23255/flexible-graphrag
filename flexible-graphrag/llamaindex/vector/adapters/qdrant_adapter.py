"""LlamaIndex Qdrant vector store adapter."""
from __future__ import annotations
from typing import Dict, Any, Optional
import logging

from llamaindex.vector.vector_store_factory import LlamaIndexVectorAdapter

logger = logging.getLogger(__name__)


class LlamaIndexQdrantAdapter(LlamaIndexVectorAdapter):
    """LlamaIndex vector store adapter backed by Qdrant.

    Configuration keys
    ------------------
    host             Qdrant host (default ``localhost``)
    port             Qdrant REST port (default ``6333``)
    api_key          API key for Qdrant Cloud (optional)
    https            Use HTTPS (default ``False``)
    collection_name  Collection to use (default ``hybrid_search``)
    """

    def __init__(self, config: Dict[str, Any], embed_dim: Optional[int] = None):
        from llama_index.vector_stores.qdrant import QdrantVectorStore
        from llama_index.vector_stores.qdrant.base import DEFAULT_DENSE_VECTOR_NAME
        from qdrant_client import QdrantClient, AsyncQdrantClient
        from qdrant_client.http import models as rest

        host = config.get("host", "localhost")
        port = config.get("port", 6333)
        collection_name = config.get("collection_name", "hybrid_search")

        client = QdrantClient(
            host=host, port=port,
            api_key=config.get("api_key"),
            https=config.get("https", False),
            check_compatibility=False,
        )
        aclient = AsyncQdrantClient(
            host=host, port=port,
            api_key=config.get("api_key"),
            https=config.get("https", False),
            check_compatibility=False,
        )

        # Drop the collection if it contains any points from a different backend
        # (e.g. LangChain) that lack LlamaIndex's '_node_content' / 'text' payload
        # keys.  Collections can be mixed (some LI points, some LC points), so we
        # check ALL points in the first batch — if ANY lacks both keys, drop.
        try:
            if client.collection_exists(collection_name):
                sample = client.scroll(collection_name, limit=20, with_payload=True)
                points = sample[0]
                if points:
                    bad = [
                        p for p in points
                        if "_node_content" not in (p.payload or {})
                        and "text" not in (p.payload or {})
                    ]
                    if bad:
                        logger.info(
                            "Qdrant collection '%s' has %d/%d non-LlamaIndex-format points "
                            "— deleting for fresh creation.",
                            collection_name, len(bad), len(points),
                        )
                        client.delete_collection(collection_name)
        except Exception as _exc:
            logger.debug("Qdrant format check skipped: %s", _exc)

        # llama-index-vector-stores-qdrant 0.10+ always upserts with a *named*
        # vector key (DEFAULT_DENSE_VECTOR_NAME = "text-dense"), but its
        # non-hybrid _create_collection path sends vectors_config=VectorParams(...)
        # which creates an *unnamed* collection — causing a 400 VectorsConfig
        # mismatch on the first upsert.
        #
        # Fix: pre-create the collection with the same named key the upsert will
        # use.  QdrantVectorStore.__init__ then detects the collection as existing
        # and skips its own (broken) creation path.
        if embed_dim and not client.collection_exists(collection_name):
            try:
                client.create_collection(
                    collection_name=collection_name,
                    vectors_config={
                        DEFAULT_DENSE_VECTOR_NAME: rest.VectorParams(
                            size=embed_dim,
                            distance=rest.Distance.COSINE,
                        )
                    },
                )
                logger.info(
                    "LlamaIndexQdrantAdapter: pre-created '%s' with named vector "
                    "'%s' dim=%d", collection_name, DEFAULT_DENSE_VECTOR_NAME, embed_dim,
                )
            except Exception as _cex:
                logger.debug("Qdrant pre-create skipped: %s", _cex)

        store = QdrantVectorStore(client=client, aclient=aclient, collection_name=collection_name)
        super().__init__(store)
        logger.info("LlamaIndexQdrantAdapter: collection=%s at %s:%s",
                    collection_name, host, port)


__all__ = ["LlamaIndexQdrantAdapter"]
