# ============================================================
# A/B Test: Polars occurrence loading and packing strategies
# ============================================================
#
# QUESTION: Fastest way to load 528M occurrence rows from geoparquet
#           into a compact 444k-row format (one row per taxon)?
#
# RESULTS:
#   Polars struct packing (baseline):
#     146s → 436k rows with STRUCT(lng, lat, elevation)[] per taxon
#
#   Polars explicit column select:
#     131s → same shape
#     10% faster from projection pushdown
#
#   Polars parallel arrays (lng[], lat[], elev[] separate columns):
#     122s → 436k rows
#     WINNER — 16% faster than baseline, skips struct packing overhead
#
#   DuckDB native (load + spatial + array_agg):
#     81s pack time BUT 240s total (parquet load into DuckDB table is slow)
#     321s total vs 122s polars total
#
#   Polars direct to habitat (skip compact, compute tile_id inline):
#     234s → 39.5M habitat rows
#     SLOWER — Mercator math on 512M rows + large group_by more expensive
#     than two-phase (pack then list.eval)
#
# WHY POLARS BEATS DUCKDB FOR LOADING:
#   - Parquet reading: polars scans columnar parquet natively, DuckDB must
#     load into row-oriented tables via CREATE TABLE AS SELECT
#   - WKB extraction: polars bin.slice(5,8).bin.reinterpret(dtype=Float64)
#     is zero-copy binary reinterpretation. DuckDB needs INSTALL spatial +
#     ST_X(location) which constructs GEOMETRY objects first
#   - Group-by packing: polars group_by with list aggregation is highly
#     optimized for this exact pattern
#
# GOTCHAS:
#   - polars scan_parquet() doesn't accept columns= kwarg (use .select() after scan)
#   - The occurrences.parquet stores coordinates as GEOMETRY (WKB binary), not
#     separate float columns. Polars sees this as Binary dtype.
#   - WKB POINT layout (little-endian): [1 byte endian][4 bytes type][8 bytes X f64][8 bytes Y f64]
#     Total 21 bytes. lng = bytes 5..13, lat = bytes 13..21
#   - bin.reinterpret(dtype=pl.Float64) — NOT bin.reinterpret(signed=True)
#   - polars-st extension exists but is not needed — raw binary slicing is sufficient
#   - is_in() filter with a Python set works for accepted taxa filtering
#
# MEMORY NOTES:
#   - Eager collect of 512M filtered rows into group_by needs ~10GB RAM
#   - Streaming alternative (.collect(engine="streaming")) adds ~8s (122s → 130s)
#     but is memory-safe for 16GB machines
#   - Minimum recommended RAM: 16GB
#
# Run: .venv/Scripts/python -X utf8 abtest/04_polars_load_pack.py

import polars as pl
import time

OCCURRENCES = 'data/geo/occurrences.parquet'
RELEASE = 'data/releases/20260328-ac238ab708c1/20260328-ac238ab708c1.parquet'

def main():
	t0 = time.time()

	# Load accepted taxa
	accepted_set = set(
		pl.scan_parquet(RELEASE)
		.filter(pl.col("accepted"), pl.col("gbif_id").is_not_null())
		.select("gbif_id").collect()["gbif_id"].to_list()
	)
	print(f"Loaded {len(accepted_set):,} accepted gbif_ids")

	# Best approach: parallel arrays
	t = time.time()
	compact = (
		pl.scan_parquet(OCCURRENCES)
		.select("taxon", "location", "elevation", "spatial_issue")
		.filter(
			~pl.col("spatial_issue"),
			pl.col("taxon").is_in(accepted_set),
		)
		.with_columns(
			pl.col("location").bin.slice(5, 8).bin.reinterpret(dtype=pl.Float64).round(3).cast(pl.Float32).alias("lng"),
			pl.col("location").bin.slice(13, 8).bin.reinterpret(dtype=pl.Float64).round(3).cast(pl.Float32).alias("lat"),
		)
		.filter(
			~((pl.col("lng") == 0.0) & (pl.col("lat") == 0.0)),
			~((pl.col("lng") == 1.0) & (pl.col("lat") == 1.0)),
		)
		.select("taxon", "lng", "lat", "elevation")
		.group_by("taxon")
		.agg(pl.col("lng"), pl.col("lat"), pl.col("elevation"))
		.collect()
	)
	elapsed = time.time() - t
	total_occ = compact.select(pl.col("lng").list.len().sum()).item()
	print(f"[{elapsed:.0f}s] {len(compact):,} taxa, {total_occ:,} occurrences")
	print(f"Schema: {compact.schema}")

	print(f"\n=== TOTAL: {time.time()-t0:.0f}s ===")

if __name__ == '__main__':
	main()
