# Canopy

Canopy builds a unified taxonomy release from multiple botanical and mycological authorities. It keeps source provenance visible, reconciles differences in acceptance and classification, and produces practical outputs for search, hierarchy browsing, and geospatial enrichment.

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

## Setup & Secrets

Run `./setup.sh` once and `./update.sh` occasionally. Copy `config/secrets.py.template` to `config/secrets.py` and fill credentials before running. Missing credentials are handled gracefully (GBIF & Wikipedia abstract updates are skipped, S3 mode disabled).