# Tropicos Dataset Contract

## 1) Role in the importer
**Tropicos** is a supplemental botanical authority used for coverage and ID linking.

Primary value:
- additional plant records (`tropicos_id`)
- supportive nomenclatural metadata
- bridge via cross-IDs from other sources

---

## 2) Data flow (what matters)

### Extracts from source
From Tropicos archives (multiple files merged):
- taxonomy/name core fields
- inferred or reconstructed rank context where source is irregular
- author/year/publication-like fields where available
- `tropicos_id` identity anchor

### Consumed by fuse
Fuse currently uses Tropicos for:
- additional row insertion in map stage
- ID backfill where `tropicos_id` links appear in other datasets
- consensus support for shared nomenclatural fields

### Extracted but currently dropped downstream
- Several Tropicos-specific metadata fields are retained only at processed-stage scope.

---

## 3) Trust model
- **Identity linking: MEDIUM**  
  Useful supplemental anchor, not primary authority.

- **Acceptance: LOW-MEDIUM**  
  Not the primary acceptance source in current contract.

- **Publication/year: MEDIUM (quality variable)**  
  Helpful but uneven and needs sanity checks.

---

## 4) Edge cases and operational learnings
- Input comes from multiple Tropicos archives and requires merge logic.
- Rank reconstruction and early-year anomalies require defensive handling.
- Tropicos is intentionally treated as supporting authority, not sole arbiter.

---

## 5) Contract matrix (feature-level)
| Feature group | Extracted in process | Used in fuse | Surfaces in distill/export | Notes |
|---|---|---|---|---|
| Backbone row + `tropicos_id` | Yes | Yes | Yes | Supplemental authority |
| Name/rank/author/year signals | Yes | Yes | Yes | Consensus support |
| Tropicos-specific extras | Partial | Limited | Limited | Not broadly surfaced |

---

## 6) Open decisions
1. Add stronger validation for suspicious pre-modern year values.
2. Decide whether Tropicos-only bibliographic fields should surface downstream.
