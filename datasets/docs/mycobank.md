# MycoBank Dataset Contract

## 1) Role in the importer
**MycoBank** is a fungal authority used mainly to complement Fungorum.

Primary value:
- fungal coverage expansion
- ID bridge through `fungorum_id`
- standardized abbreviated author strings

---

## 2) Data flow (what matters)

### Extracts from source
From `MBList.xlsx` inside MycoBank zip:
- taxonomy core fields and normalized names
- `fungorum_id` mapping for cross-linking
- author and publication-related fields (with abbreviated author preference)

### Consumed by fuse
Fuse currently uses MycoBank for:
- additional fungal backbone rows
- ID-based linking to existing `fungorum_id` rows
- consensus support where fields overlap

### Extracted but currently dropped downstream
- Source-specific publication details are not currently a dedicated fused output field.

---

## 3) Trust model
- **Identity linking: HIGH (fungi bridge)**  
  Strong value when linked through `fungorum_id`.

- **Acceptance: MEDIUM**  
  Complementary fungal signal.

- **Authorship consistency: HIGH (with abbreviated authors)**  
  Abbreviated forms align better with botanical/fungal authority consensus.

---

## 4) Edge cases and operational learnings
- Source format is Excel (`read_xlsx`) rather than TSV/CSV.
- Using `Authors (abbreviated)` instead of full names materially improves author consensus matching.

---

## 5) Contract matrix (feature-level)
| Feature group | Extracted in process | Used in fuse | Surfaces in distill/export | Notes |
|---|---|---|---|---|
| Backbone row + `mycobank_id` | Yes | Yes | Yes | Supplemental fungi source |
| `fungorum_id` bridge | Yes | Yes | Yes | Primary join path |
| Author fields (abbreviated) | Yes | Yes | Yes | Better consensus alignment |
| Publication extras | Partial | Limited | Limited | Not dedicated fused field |

---

## 6) Open decisions
1. Decide whether any MycoBank-only fields should be explicitly surfaced downstream.
2. Add clearer validation around residual hybrid/rank anomalies.
