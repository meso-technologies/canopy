# IUCN Dataset Contract

## 1) Role in the importer
**IUCN Red List** is the canonical threat-status source.

Primary value:
- `iucn_status` (LC, NT, VU, EN, CR, EX, etc)
- `iucn_assessment` ID for direct Red List linkage
- additional vernacular names

---

## 2) Data flow (what matters)

### Extracts from source
From IUCN `taxon.txt`, `vernacularname.txt`, and distribution-related files:
- taxonomy core fields
- `iucn_status` mapping from threat status
- `iucn_assessment` parsed from references URL
- vernacular arrays (`lang:name`) with cleanup and language normalization

### Consumed by fuse
Fuse currently uses IUCN for:
- direct enrichment of fused rows: `iucn_status`, `iucn_assessment`
- vernacular ingestion in `reduce_vernacular`

### Extracted but currently dropped downstream
- IUCN distribution fields are not currently used due poor practical quality.

---

## 3) Trust model
- **Threat status: HIGH**  
  Canonical source for conservation status.

- **Assessment linking: HIGH**  
  Extracted assessment IDs provide stable deep links.

- **Vernacular: MEDIUM**  
  Useful supplemental names after cleanup.

- **Distribution: LOW (current contract)**  
  Source distribution fields are not currently trusted/used.

---

## 4) Edge cases and operational learnings
- Nov 2025 schema change: `vernacularname.txt` swapped `isPreferredName` and `language` positions.
- `meta.xml` inside the archive is the definitive column-order reference.
- `iucn_assessment` is extracted from `/species/<taxon>/<assessment>` URL pattern.

---

## 5) Contract matrix (feature-level)
| Feature group | Extracted in process | Used in fuse | Surfaces in distill/export | Notes |
|---|---|---|---|---|
| `iucn_status` | Yes | Yes | Yes | Core conservation field |
| `iucn_assessment` | Yes | Yes | Yes | Supports direct IUCN links |
| Vernacular | Yes | Yes | Yes | Included in reduction pipeline |
| IUCN distribution | Partial | No | No | Currently low-value in practice |

---

## 6) Open decisions
1. Decide whether any subset of IUCN distribution data is salvageable for enrichment.
2. Keep monitoring export schema changes via `meta.xml` on each breakage.
