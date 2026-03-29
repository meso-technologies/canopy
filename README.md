# Canopy

Standalone taxonomy pipeline extracted from Meso importer.

## Setup

```bash
cd data/importer/canopy
bash ./setup.sh
```

## Update

```bash
cd data/importer/canopy
bash ./update.sh
```

## Run

```bash
# Download only
uv run python -m importer.canopy.run --download

# Process + fuse + geo + APIs
uv run python -m importer.canopy.run --process --fuse --geo --apis

# Fast partial debug run
uv run python -m importer.canopy.run --debug --process --fuse
```

## Secrets

Copy `config/secrets.py.template` to `config/secrets.py` and fill credentials.
Missing credentials are handled gracefully (GBIF updates are skipped).
