# Canopy

Canopy builds a unified taxonomy from multiple botanical and mycological authorities. 

It's consensus driven, meaning that core values like authorship, year, acceptance, parentage etc are being voted on, so every value is being driven by constant changes in the underlying authority datasets.

Aside from producing a single consistent taxonomy representing the majority consensus of 550k accepted plants and 200k fungi, it includes the following highlights:

- Normalized and deduplicated vernacular names in over 300 languages, sorted by frequency across source datasets.
- GeoJSON distribution, most representative climatic centroids, hypsograms, median elevations, and native occurrence coordinates as well as child rollups for any taxon that has occurrences.
- Direct links to a variety of BHL pages (first mention, illustrations etc).
- External taxonomy IDs from 11 primary sources matched directly, plus 16 backfilled from Wikidata.
- English Wikipedia abstracts.

System requirements: runs fine on average laptops, but needs 500GB for full Wikidata/GBIF dataset parsing, and 32GB RAM recommended to compute in-memory. Total runtime about 12 minutes (excluding downloads).

## Examples

```bash
# Simple full run
uv run python -m importer.canopy.run 

# Download only, use S3 as storage instead of local
uv run python -m importer.canopy.run --download --s3

# Process + fuse + geo + APIs
uv run python -m importer.canopy.run --process --fuse --geo --apis

# Fast partial debug run
uv run python -m importer.canopy.run --debug --process --fuse
```

## Flags

- `-d`, `--dataset` — limit source processing to one dataset (`ipni`, `fungorum`, `wcvp`, `powo`, `wfo`, `col`, `tropicos`, `mycobank`, `bhl`, `gbif`, `wikidata`, `wikispecies`, `inaturalist`, `iucn`, `ncbi`)
- `-v`, `--verbose` — show richer stage diagnostics
- `-f`, `--force` — recompute even when outputs already exist
- `--csv` — write CSV sidecars in addition to parquet
- `--debug` — reduced workload profile for local iteration
- `--download` — download source datasets only
- `--process` — process source datasets
- `--fuse` — build fused release output
- `--geo` — compute geospatial artifact
- `--apis` — run API-backed enrichment (Wikipedia abstracts etc)
- `--litmus` — run validation checks against packaged release
- `--diff` — compute per-source new/changed/deleted counts against previous release
- `--diff-against VERSION` — override diff baseline with an explicit release version
- `--s3` — use S3 storage backend (needs creds in `config/secrets.py`)

## Environment Variables

- `CANOPY_DATA_DIR` — override canopy data root directory (default: `canopy/data`).
- `CANOPY_USER_AGENT` — outbound HTTP `User-Agent` string for source/API requests (default: `Canopy Taxonomy Pipeline/1.0 (opensource@meso.cloud)`).
- `CANOPY_SENTRY_DSN` — optional Sentry DSN for standalone error/log reporting (default: disabled).

## Setup & Secrets

Run `./setup.sh` once and `./update.sh` occasionally. Copy `config/secrets.py.template` to `config/secrets.py` and fill credentials before running. Missing credentials are handled gracefully (GBIF & Wikipedia abstract updates are skipped, S3 mode disabled).