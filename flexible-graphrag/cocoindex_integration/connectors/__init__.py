"""Layered connector framework for the flexible-graphrag CocoIndex pipeline.

Two connector families live side-by-side, unified *by convention only* (shared
method names and shared row data types in ``connectors.rows``) — NOT via a formal
cross-family base class or Protocol:

``connectors.flexible``
    ``FlexibleConnector`` targets that wrap flexible-graphrag's own LlamaIndex /
    LangChain adapters (``FlexibleVector``, ``FlexiblePropertyGraph``,
    ``FlexibleSearch``, ``FlexibleRDFGraph``) plus ``FlexibleDataSource`` which
    reads all 14 flexible-graphrag data sources.

``connectors.cocoindex`` (added incrementally)
    ``CocoConnector`` targets/sources that use CocoIndex's own native connectors
    (``CocoQdrant``, ``CocoNeo4j``, ``CocoLocalFileSystem`` …), carrying
    ``can_read`` / ``can_write`` capability flags.

Two seams unify the families *by convention only*:

* ``connectors.rows`` — shared row/data schemas both families produce and consume.
* ``connectors.seam`` — ``None``-safe predicates (``is_coco_vector`` /
  ``is_coco_pg`` / ``is_coco_native`` / ``is_flexible`` …) that ``pipeline/app.py``
  uses to fork "native vs flexible" by family/kind base class rather than by
  concrete store class, so new native stores are never mis-routed.
"""
