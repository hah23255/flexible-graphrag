"""
Shared vector index upsert step for all ingest entry points.
"""

import functools
import logging
import time

from llama_index.core import StorageContext, VectorStoreIndex

logger = logging.getLogger(__name__)

try:
    from observability import get_tracer
    OBSERVABILITY_AVAILABLE = True
except ImportError:
    OBSERVABILITY_AVAILABLE = False
    get_tracer = None

# ingest_mode values
INSERT = "insert"    # additive append — insert_nodes / async_add (source, text)
REFRESH = "refresh"  # update by doc_id then add new — refresh_ref_docs (files re-ingest)


async def update_vector(
    system,
    nodes: list,
    loop,
    *,
    ingest_mode: str = INSERT,
) -> float:
    """Upsert nodes into the configured vector store / index.

    ingest_mode="insert"  — additive append: insert_nodes / Weaviate async_add
    ingest_mode="refresh" — update existing docs by ID, add new: refresh_ref_docs

    LC backends and first-time index creation are handled automatically regardless
    of ingest_mode.  Weaviate async_add is detected automatically for insert mode.

    Returns elapsed seconds (0.0 if vector store not configured).
    """
    if system.vector_store is None:
        logger.info("Vector search disabled")
        return 0.0

    start = time.time()
    store_type = type(system.vector_store).__name__
    logger.info(f"Updating vector index — {len(nodes)} nodes ({store_type}, mode={ingest_mode})")

    span = None
    if OBSERVABILITY_AVAILABLE:
        try:
            tracer = get_tracer(__name__)
            span = tracer.start_span("rag.vector_indexing")
            span.set_attribute("vector.num_nodes", len(nodes))
            span.set_attribute("vector.store_type", store_type)
            span.set_attribute("vector.ingest_mode", ingest_mode)
        except Exception as e:
            logger.debug(f"OTel span setup failed: {e}")
            span = None

    # Detect inner store type for async-path routing (Weaviate, Elasticsearch)
    _inner_store_type = type(getattr(system.vector_store, "_store", None)).__name__

    try:
        if hasattr(system.vector_store, "is_langchain") and system.vector_store.is_langchain():
            lc_raw_store = system.vector_store.get_store()
            lc_chunks = getattr(system, "_last_lc_chunks", None)
            if lc_chunks is not None:
                # Full LC pipe path: original LC docs from _run_langchain_chunk_pipeline.
                # No LI->LC conversion; store handles its own embedding.
                logger.info(
                    "[LC pipe] vector: add_documents %d LC chunks directly "
                    "(CHUNKER_BACKEND=langchain, no LI->LC conversion) [%s]",
                    len(lc_chunks), store_type,
                )
                await loop.run_in_executor(None, lc_raw_store.add_documents, lc_chunks)
                logger.info(
                    "[LC pipe] vector: wrote %d chunks to %s", len(lc_chunks), store_type,
                )
            else:
                # Fallback path: LlamaIndex nodes -> LC docs conversion (LI chunker backend).
                from langchain.utils import llamaindex_nodes_to_langchain_docs
                lc_docs = llamaindex_nodes_to_langchain_docs(nodes)
                logger.info(
                    "[LI pipe] vector: converted %d LI nodes -> %d LC docs -> %s add_documents",
                    len(nodes), len(lc_docs), store_type,
                )
                await loop.run_in_executor(None, lc_raw_store.add_documents, lc_docs)
                logger.info("Added %d docs to %s via LC add_documents", len(lc_docs), store_type)

        elif _inner_store_type in ("ElasticsearchStore", "OpensearchVectorStore"):
            # Elasticsearch and OpenSearch both use aiohttp internally.  Running
            # insert_nodes / VectorStoreIndex via run_in_executor creates the aiohttp
            # ClientSession in a worker-thread event loop; that session's Futures are
            # attached to the thread loop, not uvicorn's loop, causing "Future attached
            # to a different loop" on the first query.
            # Call async_add directly from this async context so the session is created
            # in the correct running loop from the start.
            li_store = system.vector_store.get_store()
            node_ids = await li_store.async_add(nodes)
            logger.info(f"Added {len(node_ids)} nodes to {_inner_store_type} via async_add")
            if system.vector_index is None:
                sc = StorageContext.from_defaults(vector_store=li_store)
                system.vector_index = VectorStoreIndex.from_vector_store(
                    li_store, storage_context=sc
                )

        elif _inner_store_type == "WeaviateVectorStore":
            # Weaviate async client must be used from the running uvicorn loop — not
            # from run_in_executor(insert_nodes).  Never access li_store.client when
            # only an async client was provided (raises SyncClientNotProvidedError).
            li_store = system.vector_store.get_store()
            aclient = getattr(li_store, "_aclient", None)
            if aclient is not None and not aclient.is_connected():
                await aclient.connect()
            node_ids = await li_store.async_add(nodes)
            logger.info("Added %d nodes to Weaviate via async_add", len(node_ids))
            if system.vector_index is None:
                sc = StorageContext.from_defaults(vector_store=li_store)
                system.vector_index = VectorStoreIndex.from_vector_store(
                    li_store, storage_context=sc
                )

        elif _inner_store_type == "LanceDBVectorStore":
            li_store = system.vector_store.get_store()
            node_ids = await li_store.async_add(nodes)
            refresh = getattr(system.vector_store, "_refresh_table", None)
            if callable(refresh):
                refresh()
            logger.info("Added %d nodes to LanceDB via async_add", len(node_ids))

        elif system.vector_index is None:
            sc = StorageContext.from_defaults(vector_store=system.vector_store)
            create_vi = functools.partial(
                VectorStoreIndex, nodes=nodes, storage_context=sc, show_progress=False
            )
            system.vector_index = await loop.run_in_executor(None, create_vi)

        elif ingest_mode == REFRESH:
            from llama_index.core import Document as LIDocument
            system.vector_index.refresh_ref_docs(
                [LIDocument(text=n.text, metadata=n.metadata) for n in nodes if hasattr(n, "text")]
            )

        else:
            await loop.run_in_executor(None, system.vector_index.insert_nodes, nodes)

        duration = time.time() - start
        if span:
            span.set_attribute("vector.latency_ms", duration * 1000)
            span.set_attribute("vector.status", "success")

    except Exception as e:
        duration = time.time() - start
        if span:
            span.set_attribute("vector.status", "error")
            span.set_attribute("vector.error", str(e))
            try: span.record_exception(e)
            except Exception: pass
        raise
    finally:
        if span:
            try: span.end()
            except Exception: pass

    logger.info(f"Vector index: {duration:.2f}s")
    return duration
