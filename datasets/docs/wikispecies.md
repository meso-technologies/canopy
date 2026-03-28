# Wikispecies Dataset Contract

## 1) Role in the importer
**Wikispecies** module currently acts as a downloader-only placeholder.

Primary value today:
- keeps local copies of key Wikispecies SQL dumps for future parsing

No active normalization/fuse integration is implemented yet.

---

## 2) Data flow (what matters)

### Extracts from source
Current behavior:
- downloads three dumps only:
  - `specieswiki-latest-page.sql.gz`
  - `specieswiki-latest-categorylinks.sql.gz`
  - `specieswiki-latest-templatelinks.sql.gz`
- no process table is built
- no parquet output from this handler

### Consumed downstream
- Fuse: no
- Distill: no
- Runtime usage: download cache only

### Extracted but currently dropped downstream
- Entire downloaded SQL content remains unused until parser implementation exists.

---

## 3) Trust model
- **Current production value: LOW**  
  Downloader-only stage with no active enrichment output.

- **Future potential: MEDIUM-HIGH**  
  Could improve taxon metadata/linking once parser is implemented.

---

## 4) Edge cases and operational learnings
- Because this is download-only, status appears healthy even though no enrichment output changes.
- Any future implementation should define strict parsing scope before adding heavy SQL processing.

---

## 5) Contract matrix (feature-level)
| Feature group | Extracted in process | Used in fuse | Surfaces in distill/export | Notes |
|---|---|---|---|---|
| SQL dump download | Yes | No | No | Current only behavior |
| Parsed taxon/link fields | No | No | No | Not implemented |

---

## 6) Open decisions
1. Decide whether to parse Wikispecies dumps or keep relying on Wikidata `wikispecies` links.
2. If implemented, define minimal high-value fields before full parser build.
