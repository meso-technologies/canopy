# NCBI Dataset Contract

## 1) Role in the importer
**NCBI taxonomy** is a broad cross-kingdom registry source. Primary value is the NCBI taxon ID, which powers live UniProt protein lookups on the taxon research panel.

Primary value:
- authoritative `ncbi_id` for every taxon we serve a research panel for
- author strings (`author_raw`) across the full tree, contributing to author consensus
- English vernacular labels curated by NCBI
- merged-taxid redirects that keep cross-source ID links intact across NCBI's taxonomic reshuffles

---

## 2) Data flow (what matters)

### Source
Direct from `ftp.ncbi.nlm.nih.gov/pub/taxonomy/new_taxdump/new_taxdump.zip`, refreshed daily.

Previously pulled via GBIF's `hosted-datasets.gbif.org/datasets/ncbi.zip` re-export, which GBIF flipped to ColDP in April 2026. The NCBI dump is stable, versioned, MD5-verifiable, and richer than GBIF's former export shape.

### Extracted in process
From `nodes.dmp`, `names.dmp`, `merged.dmp`:
- `id_raw`, `name_raw`, `name_clean`, `parent_raw` from scientific-name rows
- `rank_raw` / `rank_clean` via shared rank mapper
- `author_raw` parsed from `name_class = 'authority'` rows (prefix-stripped so consensus voter sees `"(L.) Heynh., 1842"`, not the full scientific name)
- `hybrid` / `hybridpos` via shared hybrid detection
- `vernacular[]` restricted to `common name` + `genbank common name` classes only
- `merged_from UINTEGER[]` arrays of obsolete taxids that now redirect to this row

### Consumed by fuse
- `add_ids` name-match backfill for full NCBI ID coverage on accepted rows
- `basic_consensus` author vote (NCBI now a contributor alongside the core authorities)
- `reduce_vernacular` folds NCBI vernacular into the multilingual pool
- `polish` deterministically redirects obsolete `ncbi_id` values via `merged_from[]` before nulling and name-match fallback

### Extracted but currently dropped downstream
- Nothing. All emitted columns have a concrete consumer.

### In the source dump but not ingested yet
- `typematerial.dmp` + `typeoftype.dmp` — type specimens with ICN/ICZN/ICNP/ICTV nomenclature codes
- `citations.dmp` — per-taxon pubmed IDs (could pre-populate research panel)
- `host.dmp` — theoretical host organisms (niche)
- `rankedlineage.dmp` / `fullnamelineage.dmp` — redundant with fuse's own `add_higher_ranks`
- `images.dmp` — organism images

---

## 3) Trust model
- **Identity linking: MEDIUM-HIGH**
  Stable canonical taxids, name-match backfill, and deterministic merged-taxid redirects.

- **Acceptance: LOW**
  Not a taxonomic-acceptance authority for plants/fungi; name coverage is high but includes strain/virus/clone noise at species level.

- **Author strings: MEDIUM**
  Parsed deterministically from authority rows, contributes to the author consensus vote for taxa NCBI covers.

- **Vernacular: MEDIUM**
  English-only, curated, no cross-contamination from taxonomic aliases.

- **Distribution/Bibliography: LOW**
  Not part of the active contract.

---

## 4) Edge cases and operational learnings
- BCP-like dump format: field terminator `\t|\t`, row terminator `\t|\n`. Every row has a trailing `\t|` on the last column which we rtrim before typed casts.
- `nodes.dmp` grew from 13 to 18 columns between `taxdmp.zip` and `new_taxdump.zip`; we read all 18 but only consume the first 4 to insulate against future schema expansion.
- NCBI uses the same `taxid` for a scientific name and its synonyms (synonyms differ only in `name_class`); we emit one row per accepted taxid and do not surface synonym rows.
- The hybrid `×` sign is stripped from `name_clean` by shared `find_hybrids`; `hybrid=true` + `hybridpos` are retained separately. UniProt strips the same sign, so downstream name matching is symmetric.
- Merged-taxid arrivals are common (~97k entries, growing monthly). Both existing litmus sentinels (Solanum melongena 4111, Fragaria × ananassa 3747) are active merge targets.

---

## 5) Contract matrix (feature-level)
| Feature group | Extracted in process | Used in fuse | Surfaces in distill/export | Notes |
|---|---|---|---|---|
| Backbone row + `ncbi_id` | Yes | Yes | Yes | Every accepted taxid |
| Taxonomy/rank mapping | Yes | Yes | Indirect | Full NCBI tree |
| `author_raw` | Yes | Yes (consensus vote) | Indirect | New in direct-NCBI pipeline |
| Vernacular (English) | Yes | Yes | Yes | Restricted to true vernacular classes |
| `merged_from[]` | Yes | Yes (polish redirect) | No | Sidecar, consumed only by polish |
| Hybrid flag | Yes | Yes | Yes | Shared detection |

---

## 6) Open decisions
1. Whether to ingest `typematerial.dmp` as a new `type_material[]` blob on taxon detail pages. Requires downstream schema + UI work but would surface data no other source carries at scale.
2. Whether to pre-compute `pubmed_ids[]` from `citations.dmp` to short-circuit the first paint of the research panel.
