# iNaturalist Dataset Contract

## 1) Role in the importer
**iNaturalist** is used mainly as a high-value vernacular and auxiliary acceptance source.

Primary value:
- multilingual common names
- additional cross-IDs from references (`ipni_id`, `col_id`, `eol_id`)
- supplemental taxon coverage

---

## 2) Data flow (what matters)

### Extracts from source
From iNaturalist export CSVs:
- taxonomy core fields and normalized names
- accepted marker behavior from source assumptions
- external references (`ipni_id`, `col_id`, `eol_id`, etc where parseable)
- vernacular arrays assembled from language-specific files

### Consumed by fuse
Fuse currently uses iNaturalist for:
- additional row insertion in map stage
- acceptance influence (included for plants/fungi sets)
- vernacular ingestion as a quality source in `reduce_vernacular`

### Extracted but currently dropped downstream
- Source-specific metadata beyond names, IDs, acceptance, vernacular is mostly not surfaced.

---

## 3) Trust model
- **Identity linking: MEDIUM**  
  Useful references but not as canonical as core nomenclatural authorities.

- **Acceptance: LOW-MEDIUM**  
  Helpful signal but should remain secondary.

- **Vernacular: HIGH**  
  One of the strongest practical common-name sources.

- **Distribution/Bibliography: LOW**  
  Not a primary authority in current importer contract.

---

## 4) Edge cases and operational learnings
- Vernacular extraction is language-file driven and requires robust cleanup for punctuation and aliases.
- Source has hybrid and informal-name edge cases that can challenge canonical rank/name mapping.

---

## 5) Contract matrix (feature-level)
| Feature group | Extracted in process | Used in fuse | Surfaces in distill/export | Notes |
|---|---|---|---|---|
| Backbone row + `inaturalist_id` | Yes | Yes | Yes | Supplemental coverage |
| Cross IDs (`ipni_id`, `col_id`, `eol_id`) | Yes | Yes | Yes | Useful bridge signals |
| Acceptance/status | Yes | Yes | Indirect | Secondary acceptance input |
| Vernacular | Yes | Yes | Yes | Major contribution |

---

## 6) Open decisions
1. Tighten handling for hybrid/informal naming edge cases.
2. Decide whether additional iNat metadata should be surfaced downstream.
