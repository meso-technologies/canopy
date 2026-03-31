# New geospatial pipeline — polars-native load, rollup, and habitat computation
# Replaces load_data + build_habitat_maps from geo.py with a faster polars path
# that also includes parent taxa rollup (genus, family, order, etc)
#
# Flow:
#   1. load_occurrences: polars reads geoparquet, extracts WKB coords, filters
#      to accepted taxa, packs into parallel arrays (one row per taxon)
#   2. rollup_to_parents: iteratively concatenates children's occurrence arrays
#      into parent taxa rows, walking up the taxonomy tree
#   3. build_habitat_maps: computes tile_id via Mercator math inside list.eval,
#      counts occurrences per tile via value_counts, derives tile center coords
#   4. Hands finished habitat_points to DuckDB for FAISS clustering + packaging
#
# Performance (528M occurrences, 444k taxa):
#   Step 1: ~115-122s
#   Step 2: ~8-10s
#   Step 3: ~18-48s
#   Total:  ~150-170s (vs 287s current DuckDB-only, species-only)

# Filesystem helpers
import os
# Mercator projection constants
import math
# Polars for columnar processing
import polars as pl
# Canopy settings and path constants
from .. import settings, RELEASES_DIR, GEO_DIR

# Mercator latitude limit — beyond this the projection is undefined
MAX_LAT = 85.0511
# QuadKey zoom level — level 10 gives ~39km × 20km tiles at the equator
TILE_LEVEL = 10
# Number of tiles per axis at this zoom level
TILE_COUNT = 2 ** TILE_LEVEL

# Reverse tile_id to tile center longitude
def tile_center_lng(tile_id: pl.Expr) -> pl.Expr:
	# tile_x = tile_id // TILE_COUNT, center at +0.5
	return (((tile_id // TILE_COUNT).cast(pl.Float64) + 0.5) / TILE_COUNT * 360.0 - 180.0).round(3)

# Reverse tile_id to tile center latitude via inverse Mercator
def tile_center_lat(tile_id: pl.Expr) -> pl.Expr:
	# tile_y = tile_id % TILE_COUNT, center at +0.5
	n = math.pi - 2.0 * math.pi * ((tile_id % TILE_COUNT).cast(pl.Float64) + 0.5) / TILE_COUNT
	# inverse Mercator: lat = arctan(sinh(n))
	return ((n.exp() - (n * -1).exp()) * 0.5).arctan().degrees().round(3)

# Load occurrences from geoparquet, extract coordinates, filter to accepted taxa, pack per taxon
def load_occurrences(release: dict) -> pl.DataFrame:
	# Announce load stage
	print(f"IMPORT : ############### Loading and packing occurrences ###############")
	# Resolve release parquet for accepted taxa lookup
	release_parquet = os.path.join(RELEASES_DIR, release.get('version'), f"{release.get('version')}.parquet")
	# Load accepted gbif_ids from release
	accepted_set = set(
		pl.scan_parquet(release_parquet)
		.filter(pl.col("accepted"), pl.col("gbif_id").is_not_null())
		.select("gbif_id").collect()["gbif_id"].to_list()
	)
	# Log accepted taxa count
	print(f"IMPORT : Loaded {len(accepted_set):,} accepted gbif_ids from release")
	# Build compact occurrence table — one row per taxon with parallel coordinate arrays
	# WKB POINT layout: bytes 5..13 = longitude (f64), bytes 13..21 = latitude (f64)
	compact = (
		pl.scan_parquet(os.path.join(GEO_DIR, 'occurrences.parquet'))
		# Only read columns we need
		.select("taxon", "location", "elevation", "spatial_issue")
		# Filter spatial issues and non-accepted taxa
		.filter(
			~pl.col("spatial_issue"),
			pl.col("taxon").is_in(accepted_set),
		)
		# Extract coordinates from WKB binary via zero-copy reinterpretation
		.with_columns(
			pl.col("location").bin.slice(5, 8).bin.reinterpret(dtype=pl.Float64).round(3).cast(pl.Float32).alias("lng"),
			pl.col("location").bin.slice(13, 8).bin.reinterpret(dtype=pl.Float64).round(3).cast(pl.Float32).alias("lat"),
		)
		# Filter null island (0,0) and (1,1) sentinels
		.filter(
			~((pl.col("lng") == 0.0) & (pl.col("lat") == 0.0)),
			~((pl.col("lng") == 1.0) & (pl.col("lat") == 1.0)),
		)
		# Drop columns no longer needed
		.select("taxon", "lng", "lat", "elevation")
		# Pack into parallel arrays per taxon
		.group_by("taxon")
		.agg(pl.col("lng"), pl.col("lat"), pl.col("elevation"))
		.collect()
	)
	# Log compact table stats
	total_occ = compact.select(pl.col("lng").list.len().sum()).item()
	print(f"IMPORT : Packed {total_occ:,} occurrences into {len(compact):,} taxa")
	# Return compact occurrence table
	return compact

# Roll up species occurrences to parent taxa by concatenating children's arrays
def rollup_to_parents(compact: pl.DataFrame, release: dict) -> pl.DataFrame:
	# Announce rollup stage
	print(f"IMPORT : ############### Rolling up occurrences to parent taxa ###############")
	# Resolve release parquet for parentage lookup
	release_parquet = os.path.join(RELEASES_DIR, release.get('version'), f"{release.get('version')}.parquet")
	# Build direct parentage table: child gbif_id → parent gbif_id (one level only)
	parentage = (
		pl.scan_parquet(release_parquet)
		.filter(pl.col("accepted"), pl.col("gbif_id").is_not_null())
		.select("gbif_id", "parent_consensus")
		.collect()
		.join(
			pl.scan_parquet(release_parquet)
			.filter(pl.col("accepted"), pl.col("gbif_id").is_not_null())
			.select(pl.col("id_meso"), pl.col("gbif_id").alias("parent_gbif"))
			.collect(),
			left_on="parent_consensus", right_on="id_meso", how="inner"
		)
		.select("gbif_id", "parent_gbif")
		# Exclude self-references
		.filter(pl.col("gbif_id") != pl.col("parent_gbif"))
	)
	# Log parentage edges
	print(f"IMPORT : Loaded {len(parentage):,} parent-child edges")
	# Iterative rollup — one taxonomic level at a time
	level = 0
	while True:
		level += 1
		# Find children in compact whose parents are not yet in compact
		existing_taxa = set(compact["taxon"].to_list())
		new_parents = (
			compact.join(parentage, left_on="taxon", right_on="gbif_id", how="inner")
			.filter(~pl.col("parent_gbif").is_in(existing_taxa))
		)
		# Stop when no new parents found
		if len(new_parents) == 0: break
		# Concatenate all children's arrays per parent
		parent_rows = new_parents.group_by("parent_gbif").agg(
			pl.col("lng").list.explode(),
			pl.col("lat").list.explode(),
			pl.col("elevation").list.explode(),
		).rename({"parent_gbif": "taxon"})
		# Append parent rows to compact table
		compact = pl.concat([compact, parent_rows], how="vertical")
		# Log progress
		print(f"IMPORT : Level {level}: added {len(parent_rows):,} parent taxa -> {len(compact):,} total")
	# Log final rollup stats
	print(f"IMPORT : Rollup complete: {len(compact):,} taxa")
	# Return compact table with parent taxa included
	return compact

# Build habitat_points from compact occurrence arrays using tile math + value_counts
def build_habitat_maps(compact: pl.DataFrame) -> pl.DataFrame:
	# Announce habitat computation stage
	print(f"IMPORT : ############### Building habitat maps ###############")
	# Compute tile coordinates and count occurrences per tile using list.eval
	# This never explodes the full 512M points — processes each taxon's list independently
	habitat = (
		compact
		# Compute tile_x from longitude and tile_y from latitude inside each list
		.with_columns(
			# tile_x: Mercator X projection
			pl.col("lng").list.eval(
				((pl.element().cast(pl.Float64) + 180.0) / 360.0 * TILE_COUNT).clip(0, TILE_COUNT - 1).cast(pl.UInt32)
			).alias("tx"),
			# tile_y: Mercator Y projection with latitude clamping
			pl.col("lat").list.eval(
				((1.0 - ((pl.element().cast(pl.Float64).clip(-MAX_LAT, MAX_LAT) * (math.pi / 180.0)).tan() +
					(1.0 / (pl.element().cast(pl.Float64).clip(-MAX_LAT, MAX_LAT) * (math.pi / 180.0)).cos())).log()
					/ math.pi) / 2.0 * TILE_COUNT).clip(0, TILE_COUNT - 1).cast(pl.UInt32)
			).alias("ty"),
		)
		# Combine tx and ty into single tile_id, then count occurrences per tile
		.with_columns(
			(pl.col("tx").list.eval(pl.element() * TILE_COUNT) + pl.col("ty"))
			.list.eval(pl.element().value_counts(sort=True))
			.alias("tile_counts")
		)
		# Explode the small tile_counts list (~60 per taxon avg) into flat rows
		.select("taxon", "tile_counts")
		.explode("tile_counts")
		.unnest("tile_counts")
		# Rename value_counts output columns
		.rename({"": "tile_id", "taxon": "gbif_id"})
		# Derive center coordinates from tile_id via reverse Mercator math
		.with_columns(
			tile_center_lng(pl.col("tile_id")).alias("center_lng"),
			tile_center_lat(pl.col("tile_id")).alias("center_lat"),
		)
	)
	# Log habitat stats
	print(f"IMPORT : Computed {len(habitat):,} habitat tiles across {habitat['gbif_id'].n_unique():,} taxa")
	# Return flat habitat_points table
	return habitat
