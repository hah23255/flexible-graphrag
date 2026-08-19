# SurrealDB — Surrealist Query & Graph Guide

This guide explains how to connect Surrealist to your SurrealDB instance after an ingest and how
to query and visualise the graph. The table schema differs depending on which graph backend you
configured (`GRAPH_BACKEND=cocoindex` vs `GRAPH_BACKEND=langchain`).

---

## Docker Setup

SurrealDB and Surrealist are started together:

```powershell
docker compose -f docker/docker-compose.yaml -f docker/includes/surrealdb.yaml -p flexible-graphrag up -d
```

| Service    | URL                          | Notes                         |
|------------|------------------------------|-------------------------------|
| SurrealDB  | `ws://localhost:8010/rpc`    | WebSocket RPC endpoint        |
| SurrealDB  | `http://localhost:8010`      | HTTP endpoint                 |
| Surrealist | http://localhost:8011        | Web UI (served by nginx)      |

Default credentials: `root` / `root`

---

## Connecting Surrealist (Critical Steps)

### 1. Create a connection

In Surrealist click **+ New connection** and fill in:

| Field      | Value                     |
|------------|---------------------------|
| URL        | `ws://localhost:8010/rpc` |
| Username   | `root`                    |
| Password   | `root`                    |
| Namespace  | `test`                    |
| Database   | `flexible_graphrag`       |

### 2. Select the namespace/database in Settings (required)

This is the step that makes the Explorer tab and Graph view work. Even though you entered the
namespace and database in the connection dialog, you must also **select** them in the Settings panel:

1. Click the **gear icon (Settings)** in the left sidebar
2. Choose **Databases**
3. Expand the **`test`** namespace
4. Click **`flexible_graphrag`** so it shows **Selected**

Without this step the Explorer tab shows an empty database and the Graph view renders nothing.

### 3. Versions

Surrealist 3.9.5+ is required for full SurrealDB v3 compatibility. If you are on an older image,
update `docker/includes/surrealdb.yaml`:

```yaml
image: surrealdb/surrealist:3.9.5   # was 3.8.3
image: surrealdb/surrealdb:v3.2.0   # was v3.0.5
```

Alternatively use the always-current web version at **https://surrealist.app**.

---

## Schema: CocoIndex backend (`GRAPH_BACKEND=cocoindex`)

This is the native CocoIndex schema. Entities all share one table; relations each get their own
edge table.

### Node tables

| Table          | Contents                                              |
|----------------|-------------------------------------------------------|
| `graph_chunk`  | Text chunks — `id`, `text`, `file_name`, `doc_id`, `embedding` |
| `graph_entity` | All entity types — `name`, `entity_type`, `entity_labels`, `doc_id`, `embedding` |

### Edge tables

| Table                     | Contents                                    |
|---------------------------|---------------------------------------------|
| `mentions`                | `graph_chunk` → `graph_entity` (which chunk mentions which entity) |
| `relation_works_for`      | Entity → Entity                             |
| `relation_has_department` | Entity → Entity                             |
| `relation_located_in`     | …one table per extracted relation type      |
| `relation_uses_technology`| …                                           |
| *(others)*                | Named `relation_<predicate_lowercase>`      |

### Useful queries

```surql
-- All entity types ingested
SELECT entity_type, count() AS cnt FROM graph_entity GROUP BY entity_type ORDER BY cnt DESC;

-- Entities of a specific type
SELECT id, name, entity_type FROM graph_entity WHERE entity_type = 'PERSON';

-- Full-text search across entity names
SELECT id, name, entity_type FROM graph_entity WHERE string::lowercase(name) CONTAINS 'acme';

-- All relations for a named entity (outbound)
SELECT out.name AS target, predicate FROM relation_works_for WHERE in.name = 'Sarah Chen';

-- Inbound relations (who works for Acme?)
SELECT in.name AS person, predicate FROM relation_works_for WHERE out.name = 'Acme Corporation';

-- All chunks that mention a specific entity
SELECT in.text AS chunk_text, in.file_name FROM mentions WHERE out.name = 'Acme Corporation';

-- List all edge tables and counts
SELECT 'mentions' AS tbl, count() AS cnt FROM mentions GROUP ALL;
SELECT 'relation_works_for' AS tbl, count() AS cnt FROM relation_works_for GROUP ALL;

-- How many edges exist per document
SELECT doc_id, count() AS edges FROM relation_works_for GROUP BY doc_id;
```

### Graph view query (CocoIndex)

In the Surrealist Query tab, switch to **Graph** view and run:

```surql
SELECT *, in, out FROM
  relation_works_for,
  relation_has_department,
  relation_works_in_department,
  relation_uses_technology,
  relation_located_in,
  relation_attended_by,
  relation_assigned_to,
  relation_held_at,
  relation_led_by,
  relation_manages,
  relation_affiliated_with,
  relation_based_in,
  mentions
```

> **Tip:** Do not use `FETCH in, out` for the graph view — it replaces RecordID references with
> inline objects, which prevents Surrealist from drawing edges. Leave them as RecordIDs.

---

## Schema: LangChain backend (`GRAPH_BACKEND=langchain`)

The LangChain SurrealDB adapter (`langchain-surrealdb`) uses a different schema: one table per
entity type, and one relation table per predicate.

### Node tables

| Table            | Contents                                        |
|------------------|-------------------------------------------------|
| `graph_Person`   | Person entity records — `name`, `type`, properties |
| `graph_Company`  | Company entity records                          |
| `graph_source`   | Source document records                         |
| *(others)*       | Named `graph_<EntityType>`                      |

### Edge tables

| Table               | Contents                         |
|---------------------|----------------------------------|
| `relation_WORKS_FOR`| Entity → Entity (uppercase names)|
| `relation_PART_OF`  | …                                |
| *(others)*          | Named `relation_<PREDICATE>`     |

### Useful queries

```surql
-- All entity tables (graph_ prefix)
INFO FOR DB;  -- look for tables starting with graph_

-- Find a person by name (case-insensitive)
SELECT * FROM graph_Person WHERE string::lowercase(name) CONTAINS 'sarah';

-- Who works for Acme? (LangChain schema)
SELECT in.name AS person FROM relation_WORKS_FOR WHERE string::lowercase(out.name) CONTAINS 'acme';

-- All relations from a company
SELECT out.name AS target, record::tb(id) AS rel_type FROM
  relation_WORKS_FOR, relation_PART_OF, relation_LOCATED_IN
WHERE string::lowercase(in.name) CONTAINS 'acme';
```

### Graph view query (LangChain)

```surql
SELECT *, in, out FROM
  relation_WORKS_FOR,
  relation_HAS_DEPARTMENT,
  relation_LOCATED_IN,
  relation_PART_OF
```

---

## Explorer tab (visual graph browsing)

The Explorer tab is better for interactive graph exploration than the Query tab:

1. Click **Explorer** in the left sidebar (table/grid icon)
2. Select the `graph_entity` table (CocoIndex) or `graph_Person` / `graph_Company` (LangChain)
3. Click any record to open it
4. Switch to the **Graph** sub-view within the record detail panel
5. Click neighbouring nodes to expand the graph hop by hop

---

## Cleanup

> **WARNING: Cleanup is irreversible.** See [DEVELOPER-TESTING-CLEANUP.md](../../DEVELOPER/DEVELOPER-TESTING-CLEANUP.md) for all available flags and common workflows.

To wipe only the SurrealDB graph data (vector store untouched):

```powershell
cd flexible-graphrag
python ../scripts/cleanup.py --graph
```

To wipe graph + CocoIndex pipeline state (so the next ingest starts fresh):

```powershell
python ../scripts/cleanup.py --graph --cocoindex
```

To wipe graph + vector (full re-ingest):

```powershell
python ../scripts/cleanup.py --graph --vector
```

Or manually in Surrealist:

```surql
-- Remove all relation + entity + chunk tables
REMOVE TABLE IF EXISTS graph_chunk;
REMOVE TABLE IF EXISTS graph_entity;
REMOVE TABLE IF EXISTS mentions;
REMOVE TABLE IF EXISTS relation_works_for;
-- repeat for each relation table shown in INFO FOR DB
```

---

## Common issues

| Symptom | Cause | Fix |
|---------|-------|-----|
| Explorer shows no tables / "Create table" dialog | Namespace/database not selected in Settings → Databases | Select `flexible_graphrag` under `test` in Settings → Databases |
| Graph view shows 0 records, 0 edges | Using `FETCH in, out` or query returns no edge tables | Remove `FETCH`; include relation tables in `FROM` clause |
| `UNION ALL` parse error | SurrealDB does not support `UNION ALL` | Use comma-separated `FROM table1, table2` |
| `type::record()` in RELATE fails | SurrealDB v3 restriction | Use `LET $from = type::record(...); RELATE $from->...` |
| Explorer empty after Surrealist upgrade | Old connection cached | Disconnect, update connection URL, reconnect |
