# IPNI Dataset Contract

## 1) Role in the importer
**IPNI** is the core nomenclatural authority for vascular plants and one of the earliest backbone seeds.

Primary value:
- stable plant name identity and nomenclature details
- strong author/year/publication signals
- synonym structures and parent relationships
- BHL page/title hints from remarks

---

## 2) Data flow (what matters)

### Extracts from source
From the IPNI export TSVs, the handler builds:
- core taxonomy and name normalization fields
- acceptance/synonym markers and parent relations
- author/year/publication fields
- synonym struct arrays
- BHL IDs (`bhl_title`, `bhl_page`)

### Consumed by fuse
Fuse currently uses IPNI for:
- primary backbone seeding for many plant rows
- consensus voting inputs (`name/rank/author/year/parent`)
- cross-linking with WCVP/POWO/WFO/CoL via shared IDs

### Extracted but currently dropped downstream
- Direct IPNI publication detail is not explicitly surfaced as a dedicated fused publication field today.
- Source-level synonym payloads are used for logic but not exposed as raw structs in final outputs.

---

## 3) Trust model
- **Identity linking: HIGH**  
  Foundational plant nomenclature authority in current map sequence.

- **Acceptance: MEDIUM**  
  Valuable input, but acceptance decisions are multi-authority.

- **Authorship/year/publication: HIGH**  
  High practical utility in consensus voting and canonical naming.

- **Distribution: LOW**  
  Not a distribution authority.

---

## 4) Edge cases and operational learnings
- BHL identifiers are extracted from remarks patterns and can be sparse/format-sensitive.
- Synonym structures are richer than current exposed fused columns.
- Type-material locality/coordinates exist as future potential but are not part of active extraction contract yet.

---

## 5) Contract matrix (feature-level)
| Feature group | Extracted in process | Used in fuse | Surfaces in distill/export | Notes |
|---|---|---|---|---|
| Backbone row + `ipni_id` | Yes | Yes | Yes | Core plant identity anchor |
| Name/rank/author/year votes | Yes | Yes | Yes | Major consensus contributor |
| Parent/synonym structures | Yes | Yes | Indirect | Logic-heavy, minimally surfaced raw |
| Publication detail | Yes | Partly | Partly | No dedicated fused publication field yet |
| BHL page/title hints | Yes | Limited | Limited | `bhl_page` mainly reinforced via Wikidata |

---

## 6) Open decisions
1. Add explicit fused publication field using IPNI + peer sources.
2. Evaluate exposing richer synonym provenance in downstream outputs.
3. Revisit IPNI type-material extraction as a separate feature contract.
