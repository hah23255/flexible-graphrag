# Q3 Planning Kickoff

Type: MeetingNote
Date: 2026-07-06

Priya Raman led the session. Attendees were Bob Smith, Zoë Café-Lange, and
Marcus Webb.

We reviewed the Q3 roadmap and agreed the ingestion rewrite is the top
priority, with the search relevance work close behind. Marcus raised concerns
about the migration window overlapping with the customer conference.

Action items:
- Bob Smith to draft the ingestion rewrite RFC by end of week.
- Zoë Café-Lange to benchmark the current search relevance baseline.
- Marcus Webb to confirm the conference dates with marketing.

## Ingestion Rewrite Design Review

Type: MeetingNote
Date: 2026-07-13

Bob Smith ran the review. Priya Raman and Marcus Webb attended.

The RFC was well received. The group agreed to keep the existing connector
interface rather than redesign it, which cuts the estimated work roughly in
half. Open question remains around backfill ordering.

Decisions:
- bob smith to prototype the connector shim.
- Priya Raman to write up the backfill ordering options.

## Search Relevance Sync

Type: MeetingNote
Date: 2026-07-20

Zoë Café-Lange organized this one. Bob Smith and Marcus Webb joined.

Baseline numbers are in and are worse than expected on long-tail queries.
The team decided to try a hybrid retrieval approach before investing in
model fine-tuning.

Next steps:
- Zoë Café-Lange to implement hybrid retrieval behind a flag.
- Marcus Webb to assemble a long-tail evaluation set.

## Retrieval Prototype Walkthrough

Type: MeetingNote
Date: 2026-07-27

Bob walked the team through the hybrid retrieval prototype. Priya Raman and
Marcus Webb joined.

Latency is acceptable at p50 but the p99 tail is worse than the old path. The
group agreed to profile before optimising anything.

Next steps:
- Bob to write up the p99 latency numbers.
- Priya Raman to book time with the infra team.


