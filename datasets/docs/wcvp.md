# WCVP Dataset Contract

## 1) Role in the importer
**World Checklist of Vascular Plants (WCVP)** is a primary plant authority used for:
- plant backbone identity and cross-linking (`wcvp_id` / `powo_id`)
- acceptance influence for Plantae
- distribution enrichment (`native_to`, `regions`)
- basic life-history traits (`annual`, `perennial`)

WCVP is **not currently used** as a publication/bibliography authority in fusion, even though publication snippets are extracted.

---

## 2) Data flow (what matters)

### Extracts from source
From `wcvp_names.csv` and `wcvp_distribution.csv`, the handler builds:
- core taxon identity and normalization fields
- acceptance/status fields
- plant hierarchy context (`family`, `genus`, `species`, `parent_raw`)
- cross-IDs (`ipni_id`, `powo_id`)
- distribution arrays (`native_to`, `regions`)
- trait flags (`annual`, `perennial`)
- publication snippet (`publication_short`)
- review flag (`reviewed`)

### Consumed by fuse
Fuse currently uses WCVP for:
- backbone and ID linking (`wcvp_id`, `powo_id`)
- acceptance decisions (Plantae authority set)
- trait enrichment (`annual`, `perennial`)
- distribution merge (`native_to`, `regions`) with CoL and GBIF

### Extracted but currently dropped downstream
- `publication_short`
- `reviewed`
- `hybrid_type`
- `hybrid_formula`

These are retained in processed parquet but not used in fused output today.

---

## 3) Trust model
- **Identity linking: HIGH**  
  Strong `powo_id`/IPNI alignment and direct role in plant backbone insertion.

- **Acceptance: HIGH**  
  Core authority in plant acceptance decisions.

- **Distribution: HIGH**  
  Structured source distribution table; separated native/all-region aggregation is intentional.

- **Traits: MEDIUM**  
  Current extraction is binary (`annual`, `perennial`) based on text contains; useful but incomplete.

- **Publication/Bibliography: LOW (current pipeline usage)**  
  Data is extracted but not fused yet.

---

## 4) Edge cases and operational learnings
- `native_to` and `regions` are intentionally different products:
  - `native_to`: excludes introduced records
  - `regions`: includes all available area records
- Trait extraction currently discards rich source text (`lifeform_description`) in favor of binary flags.
- `climate_description` exists at high coverage in source but is not extracted yet.

---

## 5) Contract matrix (feature-level)
| Feature group | Extracted in process | Used in fuse | Surfaces in distill/export | Notes |
|---|---|---|---|---|
| Backbone IDs (`wcvp_id`, `powo_id`) | Yes | Yes | Yes | Core plant linking |
| Acceptance/status | Yes | Yes | Indirect | Affects acceptance decisions |
| Distribution (`native_to`, `regions`) | Yes | Yes | Yes | Merged with CoL + GBIF |
| Traits (`annual`, `perennial`) | Yes | Yes | Yes | Coalesced with POWO/WCVP |
| Publication snippet (`publication_short`) | Yes | No | No | Candidate for future fuse rule |
| Review flag (`reviewed`) | Yes | No | No | Informational only currently |
| Hybrid detail (`hybrid_type`, `hybrid_formula`) | Yes | No | No | Not used in current fused model |

---

## 6) Open decisions
1. Add full `lifeform` text extraction while keeping binary flags.
2. Add `climate` extraction and align with POWO climate field.
3. Decide publication fusion policy (`vote(...)` vs authority-priority `COALESCE`).
