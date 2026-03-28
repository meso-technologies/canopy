# Canopy

Standalone taxonomy pipeline extracted from Meso importer.

## Install with uv

```bash
cd data/importer/canopy
uv venv
uv pip install -e .
```

## Run

Create canopy data folders once:

```bash
mkdir -p data/{source,temp,processed,releases,geo,apis}
```

Then run canopy:

```bash
# Download only
python -m importer.canopy.run --download

# Process + fuse + geo + APIs
python -m importer.canopy.run --process --fuse --geo --apis

# Fast partial debug run
python -m importer.canopy.run --debug --process --fuse
```

## Secrets

Copy `config/secrets.py.template` to `config/secrets.py` and fill credentials.
Missing credentials are handled gracefully (GBIF updates are skipped).
