"""Flexible GraphRAG Query Summary Component for Langflow.

Combines the AI Query answer and the Hybrid Search results into a single Message so a chat
flow can show BOTH from one question. AI Query and Hybrid Search stay independent (one
ChatInput fans out to each; neither feeds the other) — this node only joins their outputs
for display / the Playground.
"""

from langflow.custom import Component
from langflow.io import HandleInput, IntInput, Output
from langflow.schema.message import Message


class FlexibleQuerySummaryComponent(Component):
    """Show the AI answer + hybrid search results together (independent inputs)."""

    display_name = "Flexible: Query Summary"
    description = "Combine AI Query answer + Hybrid Search results into one chat message (independent inputs)"
    icon = "ListChecks"
    name = "FlexibleQuerySummary"

    inputs = [
        HandleInput(name="answer", display_name="AI Answer", input_types=["Data"], required=False,
                    info="From AI Query (Answer output)."),
        HandleInput(name="search", display_name="Search Results", input_types=["Data"], required=False,
                    info="From Hybrid Search (Results output)."),
        IntInput(name="max_results", display_name="Max search results shown", value=5, advanced=True),
    ]

    outputs = [
        Output(display_name="Summary", name="summary", method="summarize"),
    ]

    @staticmethod
    def _data(val):
        for it in (val if isinstance(val, list) else [val]):
            d = getattr(it, "data", None)
            if isinstance(d, dict):
                return d
        return None

    async def summarize(self) -> Message:
        ans = self._data(self.answer)
        srch = self._data(self.search)
        lines = []

        if ans and ans.get("answer"):
            lines.append("=== AI Answer ===")
            lines.append(ans["answer"])
            srcs = ans.get("sources") or []
            if srcs:
                lines.append("")
                lines.append(f"Sources ({len(srcs)}):")
                for s in srcs:
                    lines.append(f"  - {s.get('file') or '?'} (score={s.get('score')})")
            lines.append("")

        if srch is not None:
            results = srch.get("results") or []
            lines.append(f"=== Hybrid Search ({srch.get('count', len(results))} results) ===")
            for r in results[: int(self.max_results or 5)]:
                head = f"  [{r.get('rank')}] score={r.get('score')}"
                # raw system.search shape: source = "filename | DB type"
                where = r.get("source") or r.get("file_name") or r.get("file")
                if where:
                    head += f"  {where}"
                lines.append(head)
                txt = (r.get("content") or r.get("text") or "").strip().replace("\n", " ")
                if txt:
                    lines.append(f"      {txt[:200]}")

        text = "\n".join(lines).strip() or "No results."
        self.status = text[:600]
        return Message(text=text)
