# Dataset contracts

This directory contains one contract file per importer dataset module in `data/importer/datasets/`.

## Purpose
These files document **what each source is supposed to do in the pipeline**:
- source role
- extracted feature groups
- what fuse/distill actually consume
- what is currently dropped
- edge cases and open decisions

They are intentionally feature-level contracts, not full raw schema dumps.

## File mapping
- `ipni.md` <-> `ipni.py`
- `fungorum.md` <-> `fungorum.py`
- `wcvp.md` <-> `wcvp.py`
- `powo.md` <-> `powo.py`
- `wfo.md` <-> `wfo.py`
- `col.md` <-> `col.py`
- `tropicos.md` <-> `tropicos.py`
- `mycobank.md` <-> `mycobank.py`
- `gbif.md` <-> `gbif.py`
- `wikidata.md` <-> `wikidata.py`
- `inaturalist.md` <-> `inaturalist.py`
- `iucn.md` <-> `iucn.py`
- `ncbi.md` <-> `ncbi.py`
- `bhl.md` <-> `bhl.py`
- `occurrences.md` <-> `occurrences.py`
- `wikipedia.md` <-> `wikipedia.py`
- `wikispecies.md` <-> `wikispecies.py`

## Maintenance rule
When a dataset changes, update the corresponding contract in the same PR.
Keep old inline code comments in dataset `.py` files unless there is an explicit decision to migrate them.
