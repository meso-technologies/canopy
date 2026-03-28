# POWO Dataset Contract

## 1) Role in the importer
**Plants of the World Online (POWO)** is a core plant authority paired closely with WCVP.

Primary value:
- plant backbone insertion and linking (`powo_id`, `wcvp_id` bridge)
- acceptance support
- trait extraction from `dynamicProperties`
- publication snippets

---

## 2) Data flow (what matters)

### Extracts from source
From POWO taxon/name tables:
- taxonomy core, status, parent, and normalization fields
- ID bridge fields (`powo_id`, `wcvp_id`, plus linked IDs where available)
- trait booleans from `dynamicProperties` (`annual`, `perennial`)
- publication snippets (`publication_short`)

### Consumed by fuse
Fuse currently uses POWO for:
- backbone row insertion in map stage
- `powo_id <-> wcvp_id` cross-linking and backfill
- acceptance influence for plants
- trait enrichment (`annual`, `perennial`) alongside WCVP

### Extracted but currently dropped downstream
- `publication_short`
- richer `dynamicProperties` values (e.g. climate/lifeform full strings) are not yet surfaced as full fused fields.

---

## 3) Trust model
- **Identity linking: HIGH**  
  Strong bridge with WCVP and broad plant coverage.

- **Acceptance: HIGH**  
  Core plant authority in acceptance set.

- **Traits: MEDIUM-HIGH**  
  Current boolean extraction is useful but loses detail.

- **Publication/Bibliography: LOW (current usage)**  
  Extracted but not fused.

---

## 4) Edge cases and operational learnings
- POWO input is headerless TSV and depends on explicit `read_csv` column definitions.
- `dynamicProperties` holds useful detail beyond current booleans; this is a known underused area.

---

## 5) Contract matrix (feature-level)
| Feature group | Extracted in process | Used in fuse | Surfaces in distill/export | Notes |
|---|---|---|---|---|
| Backbone IDs (`powo_id`, `wcvp_id`) | Yes | Yes | Yes | Central plant cross-link |
| Acceptance/status | Yes | Yes | Indirect | Plant acceptance decisions |
| Traits (`annual`, `perennial`) | Yes | Yes | Yes | Coalesced with WCVP |
| Publication (`publication_short`) | Yes | No | No | Candidate for publication merge |
| Full climate/lifeform detail | Partial | No | No | Open feature gap |

---

## 6) Open decisions
1. Add full `lifeform` extraction from `dynamicProperties`.
2. Add `climate` extraction and coalesce policy with WCVP.
3. Add publication fusion strategy that includes POWO.
