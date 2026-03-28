# NCBI Dataset Contract

## 1) Role in the importer
**NCBI taxonomy** is used as a broad supplemental backbone source with limited vernacular value.

Primary value:
- extra taxon coverage and IDs (`ncbi_id`)
- additional English vernacular labels

---

## 2) Data flow (what matters)

### Extracts from source
From NCBI taxon/vernacular tables:
- taxonomy core fields and rank/status mapping
- parent structure and normalized names
- English vernacular array (`en:name`)

### Consumed by fuse
Fuse currently uses NCBI mainly for:
- auxiliary coverage/ID presence
- vernacular support (English-only source-side)

### Extracted but currently dropped downstream
- Most source-specific metadata beyond taxonomy core is not surfaced.

---

## 3) Trust model
- **Identity linking: LOW-MEDIUM**  
  High volume, but taxonomy includes many non-target/noisy records.

- **Acceptance: LOW**  
  Not a primary acceptance authority for plants/fungi.

- **Vernacular: LOW-MEDIUM**  
  Useful but mostly English and sparse relative to dataset size.

- **Distribution/Bibliography: LOW**  
  Not part of active contract.

---

## 4) Edge cases and operational learnings
- Dataset contains many strain/virus/clone-like non-trinomial entries and non-target noise.
- Requires strong shared cleanup (`name_cleanup`, rank normalization) to remain useful.

---

## 5) Contract matrix (feature-level)
| Feature group | Extracted in process | Used in fuse | Surfaces in distill/export | Notes |
|---|---|---|---|---|
| Backbone row + `ncbi_id` | Yes | Limited | Yes | Mainly auxiliary |
| Taxonomy/rank mapping | Yes | Limited | Indirect | High-noise domain |
| Vernacular (English) | Yes | Yes | Yes | Supplemental only |

---

## 6) Open decisions
1. Add stricter filters for non-target NCBI taxa categories.
2. Decide whether NCBI should remain vernacular input-only in practice.
