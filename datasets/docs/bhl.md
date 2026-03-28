# BHL Dataset Contract

## 1) Role in the importer
**Biodiversity Heritage Library (BHL)** is the historical bibliography source.

Primary value:
- first known mention events per taxon name
- first illustration signals
- eastern-literature first mentions
- source title/author/year context for citation timelines

BHL does **not** participate in core fuse backbone construction.

---

## 2) Data flow (what matters)

### Extracts from source
From assembled BHL text files (`page`, `pagename`, `item`, `title`, `creator`):
- reduced first-event rows keyed by `name_clean`
- mention category (`first_mention`, `first_illustration`, eastern variants)
- year, title, author, and page identity fields

### Consumed by fuse/distill
- Fuse: no direct core-map usage.
- Distill: yes, used in `prepare_data()` to build `history` JSON per accepted taxon (joined by name).

### Extracted but currently dropped downstream
- Large amounts of non-first-event BHL data are intentionally discarded for scale.
- Some item/title metadata remains condensed rather than fully surfaced.

---

## 3) Trust model
- **Historical event signal: HIGH (for available corpus)**  
  Strong for first-mention style timeline enrichment.

- **Taxon identity linkage: MEDIUM**  
  Name-based joins can be imperfect, but useful at scale.

- **Bibliographic depth: MEDIUM-HIGH**  
  Rich source data, intentionally reduced for performance.

---

## 4) Edge cases and operational learnings
- BHL website endpoint moved behind Cloudflare; downloader now uses open-data S3 path and assembles zip.
- Corrupt partial zip assembly is auto-detected and recovered on next run.
- `force_zip64=True` is required for very large entries.
- Runtime reduction from ~hundreds of millions of mention rows to first-event rows is intentional.

---

## 5) Contract matrix (feature-level)
| Feature group | Extracted in process | Used in fuse | Surfaces in distill/export | Notes |
|---|---|---|---|---|
| First mention events | Yes | No | Yes | Drives `history` JSON |
| Illustration/eastern event types | Yes | No | Yes | Included in event payload |
| Title/author/year context | Yes | No | Yes | Citation timeline context |
| Full raw mention corpus | Partial | No | No | Intentionally reduced |

---

## 6) Open decisions
1. Consider whether direct page/title IDs should be surfaced in more frontend-facing paths.
2. Evaluate adding first-observation integrations with occurrence timelines.
