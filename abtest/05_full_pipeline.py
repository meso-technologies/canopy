# ============================================================
# Full pipeline: best performers end-to-end with verification
# ============================================================
#
# PIPELINE (all polars, no DuckDB until FAISS/packaging):
#   Step 1: Polars load + WKB extract + filter accepted + pack   115-122s
#   Step 2: Polars recursive rollup to parent taxa               8-10s
#   Step 3: Polars list.eval tile_id + value_counts              18-48s
#   TOTAL:                                                       ~150-170s
#
# vs CURRENT PRODUCTION (DuckDB only, species only, no rollup):
#   Load occurrences into DuckDB table:                          240s
#   Build habitat_maps (ST_QuadKey GROUP BY):                    47s
#   TOTAL:                                                       287s
#
# IMPROVEMENT: 1.7-1.9x faster AND includes parent taxa rollup
#
# CORRECTNESS VERIFIED:
#   - Tile counts match production exactly (Quercus robur: 9454, Welwitschia: 87)
#   - Occurrence totals match (Quercus robur: 1,487,924)
#   - Zero NULL coordinates, zero NaN
#   - Higher-rank taxa present with correct rollup data
#
# EAGER vs STREAMING:
#   Eager:     159s total — faster, needs ~16GB RAM
#   Streaming: 196s total — 23% slower, memory-safe
#   Both produce identical results. Use eager with 16GB minimum.
#
# WHAT'S LEFT (not yet integrated):
#   - FAISS clustering from habitat center_points with weights=
#     (tested: linear weights, ~182 taxa/s, p50=0.005° from production)
#   - Elevation computation (median from raw occurrences, species-level only)
#   - Package into geo table (habitat JSON + centroids + elevation)
#   - Integration into geo.py replacing current load_data + build_habitat_maps
#
# Run: .venv/Scripts/python -X utf8 abtest/05_full_pipeline.py

import polars as pl
import math
import time

OCCURRENCES = 'data/geo/occurrences.parquet'
RELEASE = 'data/releases/20260328-ac238ab708c1/20260328-ac238ab708c1.parquet'
GEO = 'data/releases/20260328-ac238ab708c1/geo.20260330.parquet'
TMP = 'data/temp'
MAX_LAT = 85.0511

def tile_center_lng(tile_id: pl.Expr) -> pl.Expr:
	return (((tile_id // 1024).cast(pl.Float64) + 0.5) / 1024 * 360.0 - 180.0).round(3)

def tile_center_lat(tile_id: pl.Expr) -> pl.Expr:
	n = math.pi - 2.0 * math.pi * ((tile_id % 1024).cast(pl.Float64) + 0.5) / 1024
	return ((n.exp() - (n * -1).exp()) * 0.5).arctan().degrees().round(3)

def main():
	t_total = time.time()

	# STEP 1: Load + extract + filter + pack
	print("STEP 1: Load + extract + filter + pack", flush=True)
	t = time.time()
	accepted_set = set(
		pl.scan_parquet(RELEASE)
		.filter(pl.col("accepted"), pl.col("gbif_id").is_not_null())
		.select("gbif_id").collect()["gbif_id"].to_list()
	)
	compact = (
		pl.scan_parquet(OCCURRENCES)
		.select("taxon", "location", "elevation", "spatial_issue")
		.filter(~pl.col("spatial_issue"), pl.col("taxon").is_in(accepted_set))
		.with_columns(
			pl.col("location").bin.slice(5, 8).bin.reinterpret(dtype=pl.Float64).round(3).cast(pl.Float32).alias("lng"),
			pl.col("location").bin.slice(13, 8).bin.reinterpret(dtype=pl.Float64).round(3).cast(pl.Float32).alias("lat"),
		)
		.filter(~((pl.col("lng") == 0.0) & (pl.col("lat") == 0.0)), ~((pl.col("lng") == 1.0) & (pl.col("lat") == 1.0)))
		.select("taxon", "lng", "lat", "elevation")
		.group_by("taxon").agg(pl.col("lng"), pl.col("lat"), pl.col("elevation"))
		.collect()
	)
	time_s1 = time.time() - t
	print(f"  [{time_s1:.0f}s] {len(compact):,} taxa, {compact.select(pl.col('lng').list.len().sum()).item():,} occurrences")

	# STEP 2: Rollup to parent taxa
	print("\nSTEP 2: Rollup", flush=True)
	t = time.time()
	parentage = (
		pl.scan_parquet(RELEASE).filter(pl.col("accepted"), pl.col("gbif_id").is_not_null())
		.select("gbif_id", "parent_consensus").collect()
		.join(
			pl.scan_parquet(RELEASE).filter(pl.col("accepted"), pl.col("gbif_id").is_not_null())
			.select(pl.col("id_meso"), pl.col("gbif_id").alias("parent_gbif")).collect(),
			left_on="parent_consensus", right_on="id_meso", how="inner"
		).select("gbif_id", "parent_gbif").filter(pl.col("gbif_id") != pl.col("parent_gbif"))
	)
	level = 0
	while True:
		level += 1
		new_parents = (compact.join(parentage, left_on="taxon", right_on="gbif_id", how="inner")
			.filter(~pl.col("parent_gbif").is_in(set(compact["taxon"].to_list()))))
		if len(new_parents) == 0: break
		parent_rows = new_parents.group_by("parent_gbif").agg(
			pl.col("lng").list.explode(), pl.col("lat").list.explode(), pl.col("elevation").list.explode()
		).rename({"parent_gbif": "taxon"})
		compact = pl.concat([compact, parent_rows], how="vertical")
		print(f"  level {level}: +{len(parent_rows):,} → {len(compact):,}")
	time_s2 = time.time() - t
	print(f"  [{time_s2:.0f}s] total")

	# STEP 3: Build habitat_points
	print("\nSTEP 3: Habitat_points", flush=True)
	t = time.time()
	habitat = (
		compact.with_columns(
			pl.col("lng").list.eval(((pl.element().cast(pl.Float64) + 180.0) / 360.0 * 1024).clip(0, 1023).cast(pl.UInt32)).alias("tx"),
			pl.col("lat").list.eval(((1.0 - ((pl.element().cast(pl.Float64).clip(-MAX_LAT, MAX_LAT) * (math.pi / 180.0)).tan() +
				(1.0 / (pl.element().cast(pl.Float64).clip(-MAX_LAT, MAX_LAT) * (math.pi / 180.0)).cos())).log() / math.pi) / 2.0 * 1024).clip(0, 1023).cast(pl.UInt32)).alias("ty"),
		)
		.with_columns(
			(pl.col("tx").list.eval(pl.element() * 1024) + pl.col("ty"))
			.list.eval(pl.element().value_counts(sort=True)).alias("tile_counts")
		)
		.select("taxon", "tile_counts").explode("tile_counts").unnest("tile_counts")
		.rename({"": "tile_id", "taxon": "gbif_id"})
		.with_columns(tile_center_lng(pl.col("tile_id")).alias("center_lng"), tile_center_lat(pl.col("tile_id")).alias("center_lat"))
	)
	time_s3 = time.time() - t
	print(f"  [{time_s3:.0f}s] {len(habitat):,} habitat_points, {habitat['gbif_id'].n_unique():,} taxa")

	# VERIFY
	print("\n--- Verification ---")
	null_cnt = habitat.filter(pl.col("center_lng").is_null() | pl.col("center_lat").is_null()).height
	nan_cnt = habitat.filter(pl.col("center_lng").is_nan() | pl.col("center_lat").is_nan()).height
	print(f"  NULL: {null_cnt}, NaN: {nan_cnt}")
	for name, gbif in [("Quercus robur", 2878688), ("Welwitschia", 5275521)]:
		tiles = len(habitat.filter(pl.col("gbif_id") == gbif))
		occ = habitat.filter(pl.col("gbif_id") == gbif).select(pl.col("count").sum()).item() if tiles > 0 else 0
		print(f"  {name}: {tiles} tiles, {occ:,} occurrences")
	for name, gbif in [("Quercus genus", 2877951), ("Magnoliopsida", 220)]:
		tiles = len(habitat.filter(pl.col("gbif_id") == gbif))
		occ = habitat.filter(pl.col("gbif_id") == gbif).select(pl.col("count").sum()).item() if tiles > 0 else 0
		print(f"  {name}: {tiles:,} tiles, {occ:,} occurrences")

	# Write
	habitat.write_parquet(f"{TMP}/hp_final.parquet")

	# SUMMARY
	print(f"\n{'='*60}")
	print(f"  Step 1: {time_s1:.0f}s")
	print(f"  Step 2: {time_s2:.0f}s")
	print(f"  Step 3: {time_s3:.0f}s")
	print(f"  TOTAL:  {time_s1 + time_s2 + time_s3:.0f}s")
	print(f"\n=== WALL TIME: {time.time()-t_total:.0f}s ===")

if __name__ == '__main__':
	main()
