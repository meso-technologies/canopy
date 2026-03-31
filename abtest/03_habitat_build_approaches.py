# ============================================================
# A/B Test: Building habitat_points from compact occurrence data
# ============================================================
#
# QUESTION: How to go from 444k compact rows (each with occurrence arrays)
#           to 40M habitat_point rows (one per taxon×tile)?
#
# RESULTS:
#   DuckDB batched UNNEST (50k taxa per batch):
#     839s (14 min) → 40,010,506 rows
#     VERDICT: Works but slow — UNNEST + spatial GROUP BY per batch
#
#   DuckDB streaming from parquet (single query):
#     OOM every time — DuckDB can't stream UNNEST of nested arrays
#     VERDICT: Dead end
#
#   Polars list.eval + value_counts (zero-explode):
#     18s → 40,010,507 rows
#     VERDICT: WINNER — 47x faster than DuckDB batched
#
#   Polars lazy streaming (explode + group_by):
#     44s → 40,010,507 rows
#     VERDICT: Works, 2.4x slower than list.eval, but memory-safe
#
#   Polars eager explode + tile computation:
#     OOM — 516M flat rows × ~24 bytes = ~12GB
#     VERDICT: Dead end on machines with <32GB RAM
#
# THE WINNING APPROACH (list.eval + value_counts):
#   1. Compute tile_id = tx * 1024 + ty inside list.eval() per taxon's point list
#      - tx = floor((lng + 180) / 360 * 1024)
#      - ty = floor((1 - ln(tan(lat_rad) + sec(lat_rad)) / π) / 2 * 1024)
#      - This is standard Web Mercator tile math at zoom level 10
#   2. Chain .list.eval(pl.element().value_counts(sort=True)) on the tile_id list
#      - Counts occurrences per tile WITHIN each taxon's list — no cross-taxon explode
#      - Memory bounded by the largest single taxon's list (~1.8M points)
#   3. Explode only the small tile_counts list (~60 tiles per taxon avg)
#      - Goes from 444k rows to 40M rows — manageable
#   4. Derive center coordinates from tile_id via reverse Mercator math
#      - center_lng = (tile_x + 0.5) / 1024 * 360 - 180
#      - center_lat = arctan(sinh(π - 2π * (tile_y + 0.5) / 1024)) in degrees
#      - Pure float arithmetic — never produces NULL or NaN
#
# GOTCHAS:
#   - Latitude must be clamped to ±85.0511° (Mercator limit) before tile computation
#   - Longitude must be clamped to ±180° for tile_x
#   - tile_x and tile_y must be clipped to [0, 1023] after computation
#   - Polars doesn't have bitwise shift operators — can't build quadkey strings
#     efficiently. Using integer tile_id (tx * 1024 + ty) instead is functionally
#     equivalent for grouping. The quadkey string is only needed if DuckDB downstream
#     requires it (it doesn't — we control the pipeline)
#   - The tile center coordinate has max ~19km offset from the actual occurrence
#     average within the tile. This is inherent to tile-center approach and acceptable
#     at level 10 tile resolution (~39km × 20km)
#
# POINT EMPTY BUG (discovered during this investigation):
#   The original DuckDB build_habitat_maps used:
#     ST_ReducePrecision(ST_Centroid(ST_Envelope_Agg(location)), 0.001)
#   ST_Centroid of a zero-area polygon (from ST_Envelope_Agg on single-point groups)
#   returns POINT EMPTY. This affected 22M of 41M habitat tiles (53%!) — over half
#   the habitat data was silently broken with NULL coordinates.
#   Fix: ST_Centroid(ST_Union_Agg(location)) works correctly for single points.
#   The polars tile_id approach avoids this entirely — no GEOMETRY objects involved.
#
# Run: .venv/Scripts/python -X utf8 abtest/03_habitat_build_approaches.py

import polars as pl
import math
import time

COMPACT = 'data/temp/compact_polars.parquet'
TMP = 'data/temp'
MAX_LAT = 85.0511

def tile_center_lng(tile_id: pl.Expr) -> pl.Expr:
	return (((tile_id // 1024).cast(pl.Float64) + 0.5) / 1024 * 360.0 - 180.0).round(3)

def tile_center_lat(tile_id: pl.Expr) -> pl.Expr:
	n = math.pi - 2.0 * math.pi * ((tile_id % 1024).cast(pl.Float64) + 0.5) / 1024
	return ((n.exp() - (n * -1).exp()) * 0.5).arctan().degrees().round(3)

def main():
	t0 = time.time()

	print("Loading compact data...", flush=True)
	t = time.time()
	compact = pl.read_parquet(COMPACT)
	print(f"  [{time.time()-t:.0f}s] {len(compact):,} rows")

	# Convert struct list to parallel arrays if needed
	if "points" in compact.columns:
		t = time.time()
		compact = compact.with_columns(
			pl.col("points").list.eval(pl.element().struct.field("lng")).alias("lng"),
			pl.col("points").list.eval(pl.element().struct.field("lat")).alias("lat"),
		).select("taxon", "lng", "lat")
		print(f"  [{time.time()-t:.0f}s] Converted to parallel arrays")

	# Build habitat via list.eval + value_counts
	print("\nBuilding habitat_points...", flush=True)
	t = time.time()
	habitat = (
		compact.with_columns(
			pl.col("lng").list.eval(
				((pl.element().cast(pl.Float64) + 180.0) / 360.0 * 1024).clip(0, 1023).cast(pl.UInt32)
			).alias("tx"),
			pl.col("lat").list.eval(
				((1.0 - ((pl.element().cast(pl.Float64).clip(-MAX_LAT, MAX_LAT) * (math.pi / 180.0)).tan() +
					(1.0 / (pl.element().cast(pl.Float64).clip(-MAX_LAT, MAX_LAT) * (math.pi / 180.0)).cos())).log() / math.pi) / 2.0 * 1024).clip(0, 1023).cast(pl.UInt32)
			).alias("ty"),
		)
		.with_columns(
			(pl.col("tx").list.eval(pl.element() * 1024) + pl.col("ty"))
			.list.eval(pl.element().value_counts(sort=True))
			.alias("tile_counts")
		)
		.select("taxon", "tile_counts")
		.explode("tile_counts")
		.unnest("tile_counts")
		.rename({"": "tile_id", "taxon": "gbif_id"})
		.with_columns(
			tile_center_lng(pl.col("tile_id")).alias("center_lng"),
			tile_center_lat(pl.col("tile_id")).alias("center_lat"),
		)
	)
	elapsed = time.time() - t
	print(f"  [{elapsed:.0f}s] {len(habitat):,} habitat_points across {habitat['gbif_id'].n_unique():,} taxa")

	# Verify
	null_count = habitat.filter(pl.col("center_lng").is_null() | pl.col("center_lat").is_null()).height
	nan_count = habitat.filter(pl.col("center_lng").is_nan() | pl.col("center_lat").is_nan()).height
	print(f"  NULL coords: {null_count}, NaN coords: {nan_count}")

	# Write
	habitat.write_parquet(f"{TMP}/hp_abtest.parquet")
	print(f"\n=== TOTAL: {time.time()-t0:.0f}s ===")

if __name__ == '__main__':
	main()
