# Wikidata Dataset Contract

## 1) Role in the importer
**Wikidata** is the cross-domain enrichment authority.

Primary value:
- cross-authority IDs at scale (`ipni`, `fungorum`, `powo`, `wfo`, `tropicos`, `iucn`, etc)
- wikipedia/wikicommons/wikispecies links
- trait/usage flags (`edible`, `toxic`, `medicinal`, `annual`, `perennial`, etc)
- very broad multilingual vernacular enrichment
- page-count signal for confidence ranking

---

## 2) Data flow (what matters)

### Extracts from source
From the Wikidata entity dump:
- core taxon identifiers and normalized names
- external authority IDs (many properties)
- rank mapping from Q-rank identifiers
- Wikipedia page map + page count + commons/species links
- boolean trait flags
- vernacular map from sitelinks, labels, aliases, and P1843 claims

### Consumed by fuse
Fuse currently uses Wikidata for:
- cross-ID backfilling across nearly all mapped authorities
- page-count enrichment (`wikidata_pagecount`)
- page links (`wikipedia_page`, `wikicommons`, `wikispecies`)
- trait flags and `bhl_page`
- vernacular merge (combined with other sources)

### Extracted but currently dropped downstream
- Many raw property-level details are intentionally not surfaced directly.
- Source-side full claim provenance/references are not exposed in final tables.

---

## 3) Trust model
- **Identity linking: HIGH (crosswalk role)**  
  Broad property coverage and strong practical bridge utility.

- **Trait flags: MEDIUM**  
  Very useful, but should remain multi-source/heuristic-aware.

- **Wikipedia linkage: HIGH**  
  Central source for page and media links.

- **Vernacular: HIGH (coverage), MEDIUM (precision)**  
  Huge multilingual breadth; cleanup/dedupe is essential.

---

## 4) Edge cases and operational learnings
- Source dump is very large and uses `aria2` + staged processing tooling.
- Resume behavior can fail with stale aria sidecars; handler includes readiness/retry logic.
- Vernacular extraction intentionally combines multiple claim channels and then removes likely scientific-name leakage.

---

## 5) Contract matrix (feature-level)
| Feature group | Extracted in process | Used in fuse | Surfaces in distill/export | Notes |
|---|---|---|---|---|
| Cross-authority IDs | Yes | Yes | Yes | Major crosswalk value |
| Wikipedia/media links | Yes | Yes | Yes | `wikipedia_page`, `wikicommons`, `wikispecies` |
| Page count | Yes | Yes | Yes | Confidence/ranking signal |
| Trait flags | Yes | Yes | Yes | Includes edible/toxic/medicinal/etc |
| Vernacular map | Yes | Yes | Yes | Merged and reduced downstream |
| Raw claims/provenance | Yes | No | No | Intentionally not exposed raw |

---

## 6) Open decisions
1. Decide whether additional high-value Wikidata properties should become first-class fused columns.
2. Define stronger confidence tiers for trait flags where source ambiguity exists.
