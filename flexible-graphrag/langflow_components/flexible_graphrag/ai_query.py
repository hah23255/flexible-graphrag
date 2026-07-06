"""Flexible GraphRAG AI Query Component for Langflow.

Independent (NOT fed by Hybrid Search). Builds its own system from .env and answers a
question via the real RAG query engine (system.get_query_engine().aquery), which lazily
builds the retriever over the persistent stores.

Two outputs (the query runs once, cached): a structured "Answer" Data (answer + sources)
and a "Message" for chat / the langflow Playground (ChatInput -> AI Query -> ChatOutput).
"""

import logging
from datetime import datetime

from langflow_components.flexible_graphrag._fg_shared import run_with_query_system

from langflow.custom import Component
from langflow.io import MessageTextInput, StrInput, Output
from langflow.schema import Data
from langflow.schema.message import Message

logger = logging.getLogger(__name__)


class FlexibleAIQueryComponent(Component):
    """LLM Q&A (RAG) over the ingested content — standalone."""

    display_name = "Flexible: AI Query"
    description = "Answer a question via RAG over the ingested stores (independent of Hybrid Search)"
    icon = "MessageSquare"
    name = "FlexibleAIQuery"

    inputs = [
        MessageTextInput(name="query", display_name="Question", info="Question to answer"),
        StrInput(name="config_path", display_name="Config (.env) path", value="",
                 advanced=True, info="Blank = backend default .env (StrInput so API tweaks apply)"),
    ]

    outputs = [
        Output(display_name="Answer", name="answer", method="run_query"),
        Output(display_name="Message", name="message", method="as_message"),
    ]

    async def _compute(self) -> dict:
        # Run the RAG query once per node execution; both outputs reuse the result.
        if getattr(self, "_fg_result", None) is not None:
            return self._fg_result

        query = (self.query or "").strip()
        if not query:
            raise ValueError("Question is required")

        # Reuse a warm cached system (built once per .env, like the backend) instead of
        # rebuilding it on every question — build_system does full LLM/embedding/store/retriever
        # setup and was the main reason flow-mode query was slower than direct mode.
        async def _run(system):
            return await system.get_query_engine().aquery(query)

        # Time the AI query (retrieval + LLM synthesis), mirroring the hybrid-search timing in
        # query_engine.search so flow-mode AI query latency shows in the logs too.
        _start = datetime.now()
        logger.info(f"Starting AI query at {_start.strftime('%H:%M:%S.%f')[:-3]}: '{query[:80]}'")
        response = await run_with_query_system(self.config_path, _run)
        _dur = (datetime.now() - _start).total_seconds()
        logger.info(f"AI query completed in {_dur:.3f}s")
        answer = str(response)

        sources = []
        for sn in getattr(response, "source_nodes", []) or []:
            meta = getattr(sn, "metadata", {}) or {}
            sources.append({
                "score": getattr(sn, "score", None),
                "file": meta.get("file_name") or meta.get("source") or meta.get("file_path"),
                "text": (getattr(sn, "text", "") or "")[:500],
            })

        self._fg_result = {"answer": answer, "sources": sources, "query": query}
        return self._fg_result

    async def run_query(self) -> Data:
        r = await self._compute()
        self.status = f"Answer ({len(r['answer'])} chars), {len(r['sources'])} source(s)."
        return Data(data=r)

    async def as_message(self) -> Message:
        r = await self._compute()
        self.status = f"Answer ({len(r['answer'])} chars), {len(r['sources'])} source(s)."
        return Message(text=r["answer"])
