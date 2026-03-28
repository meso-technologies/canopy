# CoL Dataset Contract

## 1) Role in the importer
**Catalogue of Life (CoL)** is a bridge authority for plants and fungi, used for:
- cross-authority ID linking (`fungorum_id`, `powo_id`, `wfo_id`, `tropicos_id`)
- additional acceptance support
- distribution enrichment (`native_to`, `regions`)
- vernacular enrichment (kept active even when current exports under-deliver for plants/fungi)

CoL is currently **not used** as a publication or extinction authority in fused output.

---

## 2) Data flow (what matters)

### Extracts from source
From `NameUsage.tsv`, `Distribution.tsv`, `VernacularName.tsv`, and `reference.json`:
- normalized taxonomy core and status fields
- higher-rank context columns
- external IDs parsed from `col:link`
- distribution arrays from tdwg/iso/text gazetteers
- vernacular arrays (`lang:name`)
- publication snippets and BHL title IDs
- CoL extinct flag

### Consumed by fuse
Fuse currently uses CoL for:
- row insertion / cross-linking in backbone map stage
- ID backfilling (`fungorum_id`, `powo_id`, `wfo_id`, `tropicos_id`)
- acceptance voting input
- distribution merge (`native_to`, `regions`) with WCVP + GBIF
- vernacular ingestion as one quality source in `reduce_vernacular`

### Extracted but currently dropped downstream
- `publication_short`
- `bhl_title` (from CoL side)
- `extinct`

---

## 3) Trust model
- **Identity linking: HIGH**  
  CoL `col:link` parsing gives practical cross-authority bridge IDs.

- **Acceptance: MEDIUM**  
  Useful as additional signal; not sole authority.

- **Distribution: MEDIUM-HIGH**  
  Strong when tdwg/iso present; text gazetteer requires normalization/mapping.

- **Vernacular: LOW-MEDIUM (current plant/fungi exports)**  
  Pipeline path is robust, but recent exports show weak practical coverage for Plantae/Fungi.

- **Publication/Bibliography: LOW (current usage)**  
  Extracted but not fused.

---

## 4) Edge cases and operational learnings
- Processing is intentionally limited to `plantae` and `fungi` rows.
- `Distribution.tsv` parsing requires strict settings to avoid row loss:
  - tab delimiter
  - quote disabled (`quotechar='\0'`)
  - `col:areaID` forced to `VARCHAR`
- Text-gazetteer areas are reverse-mapped through `WGSRPDLOOKUP` with normalization and alias fallback.
- Current CoL plant/fungi set is effectively accepted/provisionally accepted only.

---

## 5) Contract matrix (feature-level)
| Feature group | Extracted in process | Used in fuse | Surfaces in distill/export | Notes |
|---|---|---|---|---|
| Backbone row + `col_id` | Yes | Yes | Yes | Core bridge authority |
| External IDs (`fungorum/powo/wfo/tropicos`) | Yes | Yes | Yes | Major cross-link value |
| Acceptance/status | Yes | Yes | Indirect | Input to acceptance logic |
| Distribution (`native_to`, `regions`) | Yes | Yes | Yes | Merged with WCVP + GBIF |
| Vernacular | Yes | Yes | Yes | Included in vernacular reduction |
| Publication (`publication_short`) | Yes | No | No | Candidate for future fuse rule |
| BHL title (`bhl_title`) | Yes | No | No | Not consumed from CoL currently |
| Extinction (`extinct`) | Yes | No | No | Open decision |

---

## 6) Open decisions
1. Fuse `extinct` as informational field in meso (separate from acceptance logic).
2. Add publication merge rule for `publication_short`.
3. Decide whether CoL `bhl_title` should feed bibliography enrichment.
