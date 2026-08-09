# Nuxeo + Kafka deployment (reference copy)

A **reference copy** of a Nuxeo deployment we got working with Flexible GraphRAG's
real-time change sync, which consumes Nuxeo's **audit stream from Kafka**.

> ⚠️ **Unofficial / temporary.** These files are built on top of Angel Borroy's
> content-lake POC deployment — **[github.com/aborroy/nuxeo-deployment](https://github.com/aborroy/nuxeo-deployment)**
> (Apache-2.0) — which is a proof-of-concept, not an official Nuxeo image. They are kept
> here only to record exactly what worked. For your own deployment, follow the
> deployment-agnostic steps in
> [../../docs/DATA-SOURCES/README-nuxeo.md](../../docs/DATA-SOURCES/README-nuxeo.md)
> ("Real-Time Change Sync").

## Not standalone-runnable

The `nuxeo` service in `compose.yaml` uses `build: dockerfile: Dockerfile` with
`NUXEO_GIT_*` args — it needs the surrounding upstream repo (its `Dockerfile`,
`.env`/`.env.example`, and `config/`). So you can't `docker compose up` this folder alone.

**To run it:**
1. `git clone https://github.com/aborroy/nuxeo-deployment`
2. Apply the changes below to its `compose.yaml` (or copy this folder's `compose.yaml`
   and `config/kafka.conf` over its equivalents).
3. Follow that repo's README to build/start (it builds Nuxeo + Web UI from source).

## Files here

| File | Purpose |
|---|---|
| `compose.yaml` | The modified upstream compose (Nuxeo + Postgres + **Kafka** + optional kafka-ui) |
| `config/kafka.conf` | Dropped into `/etc/nuxeo/conf.d/` — switches Nuxeo Stream to Kafka and enables the audit stream |

## What changed vs. upstream `compose.yaml`

To publish the Nuxeo audit stream to Kafka:

1. **`nuxeo.depends_on`** — add `kafka: { condition: service_started }`.
2. **`nuxeo.volumes`** — **remove** `- nuxeo-config:/etc/nuxeo`. Nuxeo's entrypoint only
   regenerates `nuxeo.conf` (and re-applies `conf.d/*`) when `/etc/nuxeo/nuxeo.conf` is
   **absent**; persisting `/etc/nuxeo` freezes config at first run and ignores `conf.d`.
   Keeping it ephemeral makes `kafka.conf` apply on every start.
3. **`nuxeo.volumes`** — **add** `- ./config/kafka.conf:/etc/nuxeo/conf.d/kafka.conf:ro`.
4. **Add the `kafka` service** — `apache/kafka:3.8.1`, KRaft (no Zookeeper), **dual
   listener**: `kafka:29092` (internal, for the Nuxeo container) and `localhost:9092` (host,
   for the Flexible GraphRAG Python consumer). Plus an optional `kafka-ui` on `:8092`.
5. **Top-level `volumes:`** — **remove** the now-unused `nuxeo-config`.

Do **not** put a named volume on the Kafka log dir — `apache/kafka` runs as uid 1000 and a
root-owned volume breaks KRaft formatting; a dev broker needs no persistence.

## Point Flexible GraphRAG at the broker

The consumer defaults to `localhost:9092`; override in the backend `.env` if needed:

```env
NUXEO_KAFKA_BOOTSTRAP_SERVERS=localhost:9092
# NUXEO_AUDIT_TOPIC=nuxeo-audit-audit   # default
```

Then enable **auto change sync** when ingesting the Nuxeo source. See the main Nuxeo doc
for how events map to ingest actions.
