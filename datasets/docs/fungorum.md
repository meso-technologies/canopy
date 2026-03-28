# Fungorum Dataset Contract

## 1) Role in the importer
**Index Fungorum** is the core fungal nomenclatural authority in the backbone.

Primary value:
- fungal identity seeding (`fungorum_id`)
- accepted/synonym signals for fungi
- hierarchy context and publication snippets

---

## 2) Data flow (what matters)

### Extracts from source
From ChecklistBank-exported Fungorum tables:
- core taxonomy fields and normalization columns
- `fungorum_id` and parent references
- status fields mapped into shared acceptance/synonym model
- publication snippets (`publication_short`)

### Consumed by fuse
Fuse currently uses Fungorum for:
- direct fungal row insertion in backbone map stage
- fungal acceptance support
- ID linking with MycoBank and CoL
- consensus contributions for nomenclatural fields

### Extracted but currently dropped downstream
- `publication_short` is extracted but not fused into a dedicated publication column.

---

## 3) Trust model
- **Identity linking: HIGH (fungi)**  
  Primary fungal authority and key join target for MycoBank/CoL.

- **Acceptance: HIGH (fungi)**  
  Core fungal acceptance signal.

- **Publication/Bibliography: MEDIUM (current usage low)**  
  Useful snippets exist, but current fuse usage is limited.

- **Distribution/Vernacular: LOW**  
  Not a primary source for these domains.

---

## 4) Edge cases and operational learnings
- Rank/status normalization is handled through shared mapping and must keep fungus-specific variants intact.
- Some rank-in-name inconsistencies exist and are noted in handler TODOs.

---

## 5) Contract matrix (feature-level)
| Feature group | Extracted in process | Used in fuse | Surfaces in distill/export | Notes |
|---|---|---|---|---|
| Backbone row + `fungorum_id` | Yes | Yes | Yes | Core fungal anchor |
| Acceptance/status | Yes | Yes | Indirect | Important for fungal acceptance |
| Name/rank/author/year votes | Yes | Yes | Yes | Consensus contributor |
| Publication (`publication_short`) | Yes | No | No | Candidate for publication merge |

---

## 6) Open decisions
1. Include Fungorum publication snippets in fused publication consensus.
2. Add explicit validation/reporting for rank-name mismatches.
