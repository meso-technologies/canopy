# GBIF Backbone Dataset Contract

## 1) Role in the importer
**GBIF backbone** is a broad cross-kingdom source used primarily for:
- supplemental backbone coverage
- distribution enrichment (`native_to`, `regions`)
- vernacular enrichment in many languages
- acceptance support (especially as secondary signal)

---

## 2) Data flow (what matters)

### Extracts from source
From GBIF backbone tables (`Taxon.tsv`, `Distribution.tsv`, `VernacularName.tsv`, related refs):
- taxonomy core and status fields
- parent hierarchy and linked source IDs where available
- distribution arrays (`native_to`, `regions`)
- vernacular arrays (`lang:name`) via language mapping
- year/publication hints where present

### Consumed by fuse
Fuse currently uses GBIF for:
- additional row insertion/coverage in mapping stage
- acceptance influence (plants and fungi authority sets include GBIF)
- distribution merge with CoL/WCVP
- vernacular ingestion as quality source in `reduce_vernacular`

### Extracted but currently dropped downstream
- GBIF-specific publication metadata is not a dedicated fused field.
- Many source-side extra columns are intentionally not surfaced.

---

## 3) Trust model
- **Identity linking: MEDIUM**  
  Useful at scale, but noisier than primary botanical/fungal authorities.

- **Acceptance: MEDIUM**  
  Broad signal, best used in multi-authority context.

- **Distribution: HIGH**  
  One of the strongest practical distribution contributors.

- **Vernacular: HIGH**  
  Rich multilingual coverage in current pipeline usage.

---

## 4) Edge cases and operational learnings
- Backbone snapshot can be stale and contains substantial `unranked`/messy records.
- Vernacular processing includes punctuation splitting/cleanup and language normalization.
- Distribution contribution is valuable despite taxonomy noise.

---

## 5) Contract matrix (feature-level)
| Feature group | Extracted in process | Used in fuse | Surfaces in distill/export | Notes |
|---|---|---|---|---|
| Backbone row + `gbif_id` | Yes | Yes | Yes | Coverage-oriented source |
| Acceptance/status | Yes | Yes | Indirect | Secondary acceptance signal |
| Distribution (`native_to`, `regions`) | Yes | Yes | Yes | Major merged contributor |
| Vernacular | Yes | Yes | Yes | Important multilingual source |
| Publication/year extras | Partial | Limited | Limited | Not a dedicated fused publication source |

---

## 6) Open decisions
1. Define stricter confidence filters for noisy GBIF rank/status edge cases.
2. Evaluate using newer GBIF backbone snapshots when available.
