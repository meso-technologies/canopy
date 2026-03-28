# WFO Dataset Contract

## 1) Role in the importer
**World Flora Online (WFO)** is a major plant authority used for backbone expansion and cross-linking.

Primary value:
- additional plant coverage and acceptance input
- robust ID linking (`wfo_id`, `ipni_id`, `tropicos_id`)
- publication snippets and BHL title hints

---

## 2) Data flow (what matters)

### Extracts from source
From WFO CSV exports:
- taxonomy core, status, parent, normalized name fields
- `wfo_id` plus linked IDs (`ipni_id`, `tropicos_id`)
- publication snippets (`publication_short`)
- BHL title IDs parsed from relation links (`bhl_title`)

### Consumed by fuse
Fuse currently uses WFO for:
- backbone row insertion in map stage
- ID backfilling via `ipni_id` and `wfo_id`
- acceptance influence for plants

### Extracted but currently dropped downstream
- `publication_short`
- `bhl_title` from WFO source field (not currently merged into fused biblio columns)

---

## 3) Trust model
- **Identity linking: HIGH**  
  Strong practical joins to IPNI/Tropicos and WFO-native identifiers.

- **Acceptance: HIGH**  
  Included in plant acceptance authority set.

- **Publication/Bibliography: MEDIUM (current usage low)**  
  Extracted fields available, but limited fused usage today.

- **Distribution/Vernacular: LOW**  
  Not primary for these domains in current pipeline.

---

## 4) Edge cases and operational learnings
- WFO download path changed (`_uber/_uber.zip`), and handler now uses corrected URL.
- Some source IDs require normalization/cleanup fallback logic (`links_raw` recovery).

---

## 5) Contract matrix (feature-level)
| Feature group | Extracted in process | Used in fuse | Surfaces in distill/export | Notes |
|---|---|---|---|---|
| Backbone row + `wfo_id` | Yes | Yes | Yes | Plant authority contributor |
| Cross IDs (`ipni_id`, `tropicos_id`) | Yes | Yes | Yes | Important join paths |
| Acceptance/status | Yes | Yes | Indirect | Plant acceptance influence |
| Publication (`publication_short`) | Yes | No | No | Candidate for publication merge |
| BHL title (`bhl_title`) | Yes | No | No | Currently not merged |

---

## 6) Open decisions
1. Fuse WFO `bhl_title` with other bibliography signals.
2. Add WFO publication snippets to fused publication field when implemented.
