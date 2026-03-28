# Wikipedia Abstracts Dataset Contract

## 1) Role in the importer
**Wikipedia abstracts** provide human-readable summary text for accepted taxa with English pages.

Primary value:
- short descriptive abstracts for frontend and API consumption
- incremental refresh linked to current fused release

This module is a post-fuse enrichment step used during distill preparation.

---

## 2) Data flow (what matters)

### Extracts from source
From Wikipedia API (`extracts`) using release-derived page titles:
- `abstract`
- `last_checked`
- keyed by `wikidata_id` plus local taxon identity fields

### Consumed by distill
- Distill loads `wikipedia_abstracts.parquet` in `prepare_data()` and uses it as external enrichment data.
- Abstracts are refreshed periodically and persisted under `data/data/importer/apis/`.

### Extracted but currently dropped downstream
- Raw API response metadata and redirect diagnostics are not persisted beyond needed mapping.

---

## 3) Trust model
- **Descriptive text: MEDIUM-HIGH**  
  Good human-readable summaries with broad coverage.

- **Identity linkage: HIGH**  
  Anchored by `wikidata_id` and release page mapping.

- **Freshness: MEDIUM**  
  Controlled by periodic recheck cadence.

---

## 4) Edge cases and operational learnings
- Uses batched API calls and backoff handling for rate limits.
- Redirect handling maps returned titles back to requested titles.
- Missing abstracts are expected for some valid pages.

---

## 5) Contract matrix (feature-level)
| Feature group | Extracted in process | Used in fuse | Surfaces in distill/export | Notes |
|---|---|---|---|---|
| Abstract text | Yes | No | Yes | Post-fuse enrichment |
| Last-checked timestamp | Yes | No | Limited | Operational freshness tracking |
| Redirect mapping logic | Runtime | No | No | Used during download pipeline |

---

## 6) Open decisions
1. Decide whether to store language-specific abstracts beyond English.
2. Define refresh SLA by taxon popularity or staleness tiers.
