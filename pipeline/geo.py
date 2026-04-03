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
# JSON serialization for manifest updates
import json
# Garbage collector helper for releasing large Python-side objects before worker spawn
import gc
# UTC timestamp helpers for geo artifact naming
from datetime import datetime, timezone
# Multiprocessing for faster centroid clustering
import multiprocessing as mp
# Mercator projection constants
import math
# NumPy arrays for FAISS inputs
import numpy as np
# FAISS kmeans backend for centroid clustering
import faiss
# Polars for columnar processing
import polars as pl
# DuckDB for final packaging and parquet write
import duckdb
# Canopy settings and path constants
from .. import settings, RELEASES_DIR, GEO_DIR, TMP_DIR
# Release/file helpers for standalone geo execution
from ..utils.filehandlers import get_latest_release, get_file
# Occurrence updater to refresh rolling occurrence parquet before geo run
from ..datasets.occurrences import update_occurrences
# Load shared storage proxy for local/S3 transparent file operations
from ..utils.s3 import storage

# Mercator latitude limit — beyond this the projection is undefined
MAX_LAT = 85.0511
# QuadKey zoom level — level 10 gives ~39km × 20km tiles at the equator
TILE_LEVEL = 10
# Number of tiles per axis at this zoom level
TILE_COUNT = 2 ** TILE_LEVEL
# Keep FAISS vector dimensionality fixed to [lng, lat]
FAISS_DIMS = 2
# Keep FAISS neighborhood radius aligned with legacy geo clustering
FAISS_RANGE_RADIUS = 5.0
# Keep FAISS centroid diversity threshold aligned with legacy geo clustering
FAISS_DIVERSITY_DISTANCE = 9.0
# Keep raw occurrence rollup capped at family-level parent ranks
RAW_ROLLUP_PARENT_RANKS = [
	"FORM",
	"VARIETY",
	"SUBSPECIES",
	"SPECIES",
	"SUBGENUS",
	"GENUS",
	"SUBTRIBE",
	"TRIBE",
	"SUBFAMILY",
	"FAMILY",
]
# Process habitat tile computation in taxon batches to control peak memory
HABITAT_BATCH_TAXA = 20000
# Keep elevation profile granularity at 100m bins
ELEVATION_BIN_SIZE = 100
# Keep legacy-style raw fallback cutoff based on habitat tile cardinality
HABITAT_TILE_THRESHOLD = 100
# Split packaging into deterministic hash partitions to cap peak memory during habitat aggregation
PACKAGE_PARTITIONS = 16

# Compute full geospatial artifact using the new polars-based pipeline
async def compute_geospatial(release):
	# Announce start of new geo pipeline stage
	print(f"IMPORT : ############### Computing Geospatial Data ###############")
	# Fall back to latest release when none is provided
	if not release:
		# Resolve latest release manifest from releases directory
		release = get_latest_release()
		# Abort when no release candidate is available
		if not release:
			# Log missing release condition
			print(f"IMPORT : No release candidate found, aborting")
			# Stop stage because output cannot be attached
			return
		# Log fallback release selection
		else: print(f"IMPORT : No release provided, falling back on latest staging release {release['version']}")
	# Check whether a geo artifact already exists for this release
	computed_file = get_file('geo', os.path.join(RELEASES_DIR, release.get('version')))
	# Skip recomputation unless force mode was requested
	if computed_file and not settings.FORCE:
		# Log skip decision and force override hint
		print(f"IMPORT : Release {release.get('version')} already has a geo file {computed_file}, use -f to overrride")
		# Stop stage because output already exists
		return
	# Refresh rolling occurrence dataset before geospatial computation
	await update_occurrences()
	# Ensure release and occurrence parquet inputs exist locally when running in S3 mode
	if storage.is_s3():
		# Pull release parquet from S3 into release directory for local polars scans
		storage.ensure_local(os.path.join(RELEASES_DIR, release.get('version'), f"{release.get('version')}.parquet"), os.path.join(RELEASES_DIR, release.get('version')))
		# Pull rolling occurrences parquet from S3 into geo directory for local polars scans
		storage.ensure_local(os.path.join(GEO_DIR, 'occurrences.parquet'), GEO_DIR)
	# Guard against missing occurrence parquet after update
	if not os.path.isfile(os.path.join(GEO_DIR, 'occurrences.parquet')):
		# Log missing prerequisite dataset
		print('IMPORT : No occurrences.parquet available, skipping geo step')
		# Stop stage due to missing input
		return
	# Build habitat points, raw fallback arrays, and in-memory elevation payloads
	habitat_points, raw_fallback_arrays, elevation_bins, elevation_medians = build_habitat_for_release(release)
	# Resolve temporary parquet path for habitat payload
	habitat_points_path = os.path.join(TMP_DIR, f"geo_habitat_points_{release.get('version')}.parquet")
	# Resolve temporary parquet path for raw fallback payload
	raw_fallback_path = os.path.join(TMP_DIR, f"geo_raw_fallback_{release.get('version')}.parquet")
	# Resolve temporary parquet path for centroid payload
	centroids_path = os.path.join(TMP_DIR, f"geo_centroids_{release.get('version')}.parquet")
	# Resolve temporary parquet path for elevation profile payload
	elevation_bins_path = os.path.join(TMP_DIR, f"geo_elevation_bins_{release.get('version')}.parquet")
	# Resolve temporary parquet path for median elevation payload
	elevation_medians_path = os.path.join(TMP_DIR, f"geo_elevation_medians_{release.get('version')}.parquet")
	# Persist habitat payload before clustering to lower Windows spawn memory pressure
	habitat_points.write_parquet(habitat_points_path)
	# Persist raw fallback payload before clustering for legacy low-tile behavior
	raw_fallback_arrays.write_parquet(raw_fallback_path)
	# Persist elevation profile bins before clustering to reduce resident memory during worker spawn
	elevation_bins.write_parquet(elevation_bins_path)
	# Persist elevation medians before clustering to reduce resident memory during worker spawn
	elevation_medians.write_parquet(elevation_medians_path)
	# Drop in-memory habitat payload before opening FAISS multiprocessing pool
	del habitat_points
	# Drop in-memory raw fallback payload before opening FAISS multiprocessing pool
	del raw_fallback_arrays
	# Drop in-memory elevation payloads before opening FAISS multiprocessing pool
	del elevation_bins
	# Drop in-memory median payloads before opening FAISS multiprocessing pool
	del elevation_medians
	# Force a collection cycle before worker spawn to reduce Windows spawn import failures
	gc.collect()
	# Wrap clustering + packaging so temporary files are cleaned on both success and failure
	try:
		# Build representative centroid points per taxon from staged habitat and fallback parquets
		clustered_centroids = find_clusters_from_parquet(habitat_points_path, raw_fallback_path)
		# Persist centroid payload for parquet-backed packaging
		clustered_centroids.write_parquet(centroids_path)
		# Drop in-memory centroids before DuckDB packaging stage
		del clustered_centroids
		# Force collection before packaging to reduce peak RSS
		gc.collect()
		# Package habitat, centroids, and elevation outputs into final geo artifact
		package_data_from_parquet(release, habitat_points_path, centroids_path, elevation_bins_path, elevation_medians_path)
	# Always cleanup temporary staging payload files
	finally:
		# Remove temporary habitat parquet when present
		if os.path.isfile(habitat_points_path): os.remove(habitat_points_path)
		# Remove temporary raw fallback parquet when present
		if os.path.isfile(raw_fallback_path): os.remove(raw_fallback_path)
		# Remove temporary centroid parquet when present
		if os.path.isfile(centroids_path): os.remove(centroids_path)
		# Remove temporary elevation bin parquet when present
		if os.path.isfile(elevation_bins_path): os.remove(elevation_bins_path)
		# Remove temporary elevation median parquet when present
		if os.path.isfile(elevation_medians_path): os.remove(elevation_medians_path)

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
	print(f"IMPORT : Loading and packing occurrences")
	# Resolve release parquet for accepted taxa lookup
	release_parquet = os.path.join(RELEASES_DIR, release.get('version'), f"{release.get('version')}.parquet")
	# Build lazy accepted taxa table for a streaming join
	accepted_lf = (
		pl.scan_parquet(release_parquet)
		# Keep only accepted taxa with gbif ids
		.filter(pl.col("accepted"), pl.col("gbif_id").is_not_null())
		# Align column name with occurrence taxon key
		.select(pl.col("gbif_id").alias("taxon"))
		# Deduplicate gbif ids for a clean join input
		.unique()
	)
	# Collect accepted taxa count for logging
	accepted_count = accepted_lf.select(pl.len().alias("n")).collect().item()
	# Log accepted taxa count
	print(f"IMPORT : Loaded {accepted_count:,} accepted gbif_ids from release")
	# Build compact occurrence table — one row per taxon with parallel coordinate arrays
	# WKB POINT layout: bytes 5..13 = longitude (f64), bytes 13..21 = latitude (f64)
	compact = (
		pl.scan_parquet(os.path.join(GEO_DIR, 'occurrences.parquet'))
		# Only read columns we need
		.select("taxon", "location", "elevation", "spatial_issue")
		# Filter spatial issues first
		.filter(~pl.col("spatial_issue"))
		# Keep only accepted taxa via lazy join
		.join(accepted_lf, on="taxon", how="inner")
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
		# Use streaming collect to reduce peak memory at large occurrence volume
		.collect(engine="streaming")
	)
	# Log compact table stats
	total_occ = compact.select(pl.col("lng").list.len().sum()).item()
	print(f"IMPORT : Packed {total_occ:,} occurrences into {len(compact):,} taxa")
	# Return compact occurrence table
	return compact

# Build direct parentage edges with parent rank for split rollup strategy
def load_parentage(release: dict) -> pl.DataFrame:
	# Resolve release parquet for parentage lookup
	release_parquet = os.path.join(RELEASES_DIR, release.get('version'), f"{release.get('version')}.parquet")
	# Load accepted taxa with ids and parent pointers
	taxa = (
		pl.scan_parquet(release_parquet)
		.filter(pl.col("accepted"), pl.col("gbif_id").is_not_null())
		.select("gbif_id", "id_meso", "parent_consensus", "rank_consensus")
		.collect()
	)
	# Build direct child->parent edges with parent rank attached
	parentage = (
		taxa
		.select("gbif_id", "parent_consensus")
		.join(
			taxa.select(
				pl.col("id_meso"),
				pl.col("gbif_id").alias("parent_gbif"),
				pl.col("rank_consensus").alias("parent_rank"),
			),
			left_on="parent_consensus", right_on="id_meso", how="inner"
		)
		.select("gbif_id", "parent_gbif", "parent_rank")
		# Exclude self-references
		.filter(pl.col("gbif_id") != pl.col("parent_gbif"))
	)
	# Log parentage edges
	print(f"IMPORT : Loaded {len(parentage):,} parent-child edges")
	# Return parent edge table
	return parentage

# Roll up occurrences to family and below by concatenating children's arrays
def rollup_to_parents(compact: pl.DataFrame, parentage: pl.DataFrame) -> pl.DataFrame:
	# Announce lower-rank rollup stage
	print(f"IMPORT : Rolling up occurrences to family-level parents")
	# Keep only edges whose parent rank is family or below
	low_parentage = parentage.filter(pl.col("parent_rank").is_in(RAW_ROLLUP_PARENT_RANKS)).select("gbif_id", "parent_gbif")
	# Seed recursive frontier with direct occurrences for all taxa
	frontier = compact
	# Keep direct and rolled contributions for one final merge
	rollup_parts = [compact]
	# Iterative rollup — propagate child arrays upward one level at a time
	level = 0
	while True:
		# Build next frontier by grouping current frontier into direct parents
		frontier = (
			frontier
			.join(low_parentage, left_on="taxon", right_on="gbif_id", how="inner")
			.group_by("parent_gbif")
			.agg(
				pl.col("lng").list.explode(),
				pl.col("lat").list.explode(),
				pl.col("elevation").list.explode(),
			)
			.rename({"parent_gbif": "taxon"})
			.with_columns(pl.col("taxon").cast(compact.schema["taxon"]))
		)
		# Stop when no more lower-rank parents receive contributions
		if frontier.is_empty(): break
		# Track level count for logs
		level += 1
		# Save this level's contributions for final merge
		rollup_parts.append(frontier)
		# Log level progress
		print(f"IMPORT : Low rollup level {level}: rolled into {len(frontier):,} parent taxa")
	# Merge direct occurrences with all lower-rank parent contributions
	rolled = pl.concat(rollup_parts, how="vertical").group_by("taxon").agg(
		pl.col("lng").list.explode(),
		pl.col("lat").list.explode(),
		pl.col("elevation").list.explode(),
	)
	# Log lower-rank rollup stats
	print(f"IMPORT : Low rollup complete: {len(rolled):,} taxa")
	# Return compact table with lower-rank recursive aggregation applied
	return rolled

# Build habitat tile counts from one compact batch using tile math + value_counts
def build_habitat_tile_batch(compact_batch: pl.DataFrame) -> pl.DataFrame:
	# Compute tile coordinates and count occurrences per tile using list.eval
	return (
		compact_batch
		.with_columns(
			pl.col("lng").list.eval(
				((pl.element().cast(pl.Float64) + 180.0) / 360.0 * TILE_COUNT).clip(0, TILE_COUNT - 1).cast(pl.UInt32)
			).alias("tx"),
			pl.col("lat").list.eval(
				((1.0 - ((pl.element().cast(pl.Float64).clip(-MAX_LAT, MAX_LAT) * (math.pi / 180.0)).tan() +
					(1.0 / (pl.element().cast(pl.Float64).clip(-MAX_LAT, MAX_LAT) * (math.pi / 180.0)).cos())).log()
					/ math.pi) / 2.0 * TILE_COUNT).clip(0, TILE_COUNT - 1).cast(pl.UInt32)
			).alias("ty"),
		)
		.with_columns(
			(pl.col("tx").list.eval(pl.element() * TILE_COUNT) + pl.col("ty"))
			.list.eval(pl.element().value_counts(sort=True))
			.alias("tile_counts")
		)
		.select("taxon", "tile_counts")
		.explode("tile_counts")
		.unnest("tile_counts")
		.rename({"": "tile_id", "taxon": "gbif_id"})
		.select("gbif_id", "tile_id", "count")
	)

# Build habitat tile counts from compact occurrence arrays using chunked batches
def build_habitat_tiles(compact: pl.DataFrame) -> pl.DataFrame:
	# Announce habitat tile computation stage
	print(f"IMPORT : Building habitat tile counts")
	# Hold per-batch habitat outputs
	chunks = []
	# Iterate taxon batches to reduce peak allocations
	for start in range(0, len(compact), HABITAT_BATCH_TAXA):
		# Slice current compact batch
		batch = compact.slice(start, HABITAT_BATCH_TAXA)
		# Build habitat tile rows for current batch
		chunks.append(build_habitat_tile_batch(batch))
	# Merge all chunk outputs and sum duplicate keys once
	habitat_tiles = pl.concat(chunks, how="vertical").group_by("gbif_id", "tile_id").agg(pl.col("count").sum().alias("count"))
	# Log habitat tile stats
	print(f"IMPORT : Computed {len(habitat_tiles):,} habitat tiles across {habitat_tiles['gbif_id'].n_unique():,} taxa")
	# Return flat habitat tile table
	return habitat_tiles

# Build exact low-rank median elevations and low-rank elevation profile bins from arrays
def build_low_elevation_stats(compact: pl.DataFrame) -> tuple[pl.DataFrame, pl.DataFrame]:
	# Announce low-rank elevation computation stage
	print(f"IMPORT : Building low-rank elevation medians and profiles")
	# Compute exact median elevation while raw arrays still exist
	low_medians = compact.select(
		pl.col("taxon").alias("gbif_id"),
		pl.col("elevation").list.median().round(0).cast(pl.Int32).alias("elevation"),
	)
	# Compute 100m elevation profile bins with per-bin occurrence counts
	low_bins = (
		compact
		.with_columns(
			pl.col("elevation")
			.list.eval(((pl.element().cast(pl.Int32) // ELEVATION_BIN_SIZE) * ELEVATION_BIN_SIZE))
			.list.eval(pl.element().value_counts(sort=True))
			.alias("elevation_counts")
		)
		.select(pl.col("taxon").alias("gbif_id"), "elevation_counts")
		.explode("elevation_counts")
		.unnest("elevation_counts")
		.rename({"": "elevation_bin", "count": "bin_count"})
		.select("gbif_id", pl.col("elevation_bin").cast(pl.Int32), pl.col("bin_count").cast(pl.Int64))
	)
	# Log low-rank elevation stats
	print(f"IMPORT : Built low-rank elevation stats for {len(low_medians):,} taxa")
	# Return exact low medians and low profile bins
	return low_medians, low_bins

# Roll up habitat tile counts above family to avoid kingdom-scale raw occurrence arrays
def rollup_habitat_to_parents(habitat_tiles: pl.DataFrame, parentage: pl.DataFrame) -> pl.DataFrame:
	# Announce upper-rank habitat rollup stage
	print(f"IMPORT : Rolling up habitat tiles above family")
	# Keep only edges whose parent rank is above family cutoff
	high_parentage = parentage.filter(~pl.col("parent_rank").is_in(RAW_ROLLUP_PARENT_RANKS)).select("gbif_id", "parent_gbif")
	# Seed recursive frontier with all current habitat rows
	frontier = habitat_tiles.select("gbif_id", "tile_id", "count")
	# Keep direct and rolled habitat contributions for one final merge
	rollup_parts = [frontier]
	# Iterative rollup — propagate tile counts upward one level at a time
	level = 0
	while True:
		# Build next frontier by summing child tile counts into direct parents
		frontier = (
			frontier
			.join(high_parentage, on="gbif_id", how="inner")
			.group_by("parent_gbif", "tile_id")
			.agg(pl.col("count").sum().alias("count"))
			.rename({"parent_gbif": "gbif_id"})
			.with_columns(pl.col("gbif_id").cast(habitat_tiles.schema["gbif_id"]))
		)
		# Stop when no more upper-rank parents receive contributions
		if frontier.is_empty(): break
		# Track level count for logs
		level += 1
		# Save this level's contributions for final merge
		rollup_parts.append(frontier)
		# Log level progress
		print(f"IMPORT : High rollup level {level}: rolled into {frontier['gbif_id'].n_unique():,} parent taxa")
	# Merge direct habitat with all upper-rank contributions
	rolled = (
		pl.concat(rollup_parts, how="vertical")
		.group_by("gbif_id", "tile_id")
		.agg(pl.col("count").sum().alias("count"))
	)
	# Log upper-rank rollup stats
	print(f"IMPORT : High rollup complete: {rolled['gbif_id'].n_unique():,} taxa with habitat")
	# Return full recursive habitat tile table
	return rolled

# Roll up elevation profile bins above family using additive bin counts
def rollup_elevation_to_parents(low_bins: pl.DataFrame, parentage: pl.DataFrame) -> pl.DataFrame:
	# Announce upper-rank elevation rollup stage
	print(f"IMPORT : Rolling up elevation profiles above family")
	# Keep only edges whose parent rank is above family cutoff
	high_parentage = parentage.filter(~pl.col("parent_rank").is_in(RAW_ROLLUP_PARENT_RANKS)).select("gbif_id", "parent_gbif")
	# Seed recursive frontier with low-rank bins
	frontier = low_bins.select("gbif_id", "elevation_bin", "bin_count")
	# Keep direct and rolled bin contributions for one final merge
	rollup_parts = [frontier]
	# Iterative rollup — propagate bin counts upward one level at a time
	level = 0
	while True:
		# Build next frontier by summing child bin counts into direct parents
		frontier = (
			frontier
			.join(high_parentage, on="gbif_id", how="inner")
			.group_by("parent_gbif", "elevation_bin")
			.agg(pl.col("bin_count").sum().alias("bin_count"))
			.rename({"parent_gbif": "gbif_id"})
			.with_columns(pl.col("gbif_id").cast(low_bins.schema["gbif_id"]))
		)
		# Stop when no more upper-rank parents receive contributions
		if frontier.is_empty(): break
		# Track level count for logs
		level += 1
		# Save this level's contributions for final merge
		rollup_parts.append(frontier)
		# Log level progress
		print(f"IMPORT : Elevation rollup level {level}: rolled into {frontier['gbif_id'].n_unique():,} parent taxa")
	# Merge direct bins with all upper-rank contributions
	rolled = (
		pl.concat(rollup_parts, how="vertical")
		.group_by("gbif_id", "elevation_bin")
		.agg(pl.col("bin_count").sum().alias("bin_count"))
	)
	# Log upper-rank elevation rollup stats
	print(f"IMPORT : Elevation rollup complete: {rolled['gbif_id'].n_unique():,} taxa with profiles")
	# Return full recursive elevation profile table
	return rolled

# Derive median elevation integers from rolled profile bins
def medians_from_elevation_bins(all_bins: pl.DataFrame) -> pl.DataFrame:
	# Announce median derivation stage
	print(f"IMPORT : Deriving higher-rank median elevations from profiles")
	# Open in-memory DuckDB for windowed median derivation
	with duckdb.connect(':memory:') as db:
		# Route temporary spill files to canopy temp directory
		db.execute(f"SET temp_directory = '{TMP_DIR}'")
		# Register profile bins in DuckDB
		db.register('elevation_bins_df', all_bins.to_arrow())
		# Compute median bins using cumulative counts
		result = db.execute("""
			WITH ordered AS (
				SELECT
					gbif_id,
					elevation_bin,
					bin_count,
					sum(bin_count) OVER (PARTITION BY gbif_id ORDER BY elevation_bin ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS cum_count,
					sum(bin_count) OVER (PARTITION BY gbif_id) AS total_count
				FROM elevation_bins_df
			),
			picked AS (
				SELECT
					gbif_id,
					min(elevation_bin) FILTER (WHERE cum_count >= (total_count + 1) / 2.0) AS elevation
				FROM ordered
				GROUP BY gbif_id
			)
			SELECT gbif_id, cast(elevation AS INTEGER) AS elevation
			FROM picked;
		""").pl()
	# Return derived higher-rank medians
	return result

# Attach tile center coordinates to habitat tile counts for clustering and packaging
def finalize_habitat_maps(habitat_tiles: pl.DataFrame) -> pl.DataFrame:
	# Derive center coordinates from tile_id via reverse Mercator math
	habitat_points = habitat_tiles.with_columns(
		tile_center_lng(pl.col("tile_id")).alias("center_lng"),
		tile_center_lat(pl.col("tile_id")).alias("center_lat"),
	)
	# Log final habitat point stats
	print(f"IMPORT : Finalized {len(habitat_points):,} habitat points across {habitat_points['gbif_id'].n_unique():,} taxa")
	# Return habitat points table
	return habitat_points

# Build habitat, raw fallback centroid inputs, and elevation outputs for a release
# Keep raw fallback arrays only for low-tile taxa to avoid carrying all raw arrays into clustering
def build_habitat_for_release(release: dict) -> tuple[pl.DataFrame, pl.DataFrame, pl.DataFrame, pl.DataFrame]:
	# Build species-level occurrence arrays from rolling geoparquet
	occurrence_compact = load_occurrences(release)
	# Build parent edge table with rank metadata
	parentage = load_parentage(release)
	# Build lower-rank rolled occurrence arrays up to family
	occurrence_rolled = rollup_to_parents(occurrence_compact, parentage)
	# Release compact arrays immediately after lower-rank rollup stage
	del occurrence_compact
	# Build exact low-rank medians and low-rank elevation bins
	low_medians, low_bins = build_low_elevation_stats(occurrence_rolled)
	# Build per-taxon habitat tile counts from rolled occurrence arrays
	habitat_tiles = build_habitat_tiles(occurrence_rolled)
	# Roll habitat tile counts recursively above family to top-level taxa
	habitat_rolled = rollup_habitat_to_parents(habitat_tiles, parentage)
	# Build per-taxon tile cardinality from rolled habitat for weighted/raw centroid split
	tile_counts = habitat_rolled.group_by('gbif_id').agg(pl.len().alias('n_tiles'))
	# Select low-tile taxa for raw occurrence fallback clustering
	fallback_taxa = tile_counts.filter(pl.col('n_tiles') < HABITAT_TILE_THRESHOLD).select('gbif_id')
	# Build raw fallback arrays for low-tile taxa only
	raw_fallback_arrays = (
		occurrence_rolled
		.select(
			pl.col('taxon').alias('gbif_id'),
			pl.col('lng').cast(pl.List(pl.Float32)).alias('x'),
			pl.col('lat').cast(pl.List(pl.Float32)).alias('y'),
		)
		.join(fallback_taxa, on='gbif_id', how='inner')
	)
	# Log low-tile fallback taxon count
	print(f"IMPORT : Prepared {len(raw_fallback_arrays):,} raw centroid fallback taxa (<{HABITAT_TILE_THRESHOLD} tiles)")
	# Roll elevation bins recursively above family to top-level taxa
	elevation_bins = rollup_elevation_to_parents(low_bins, parentage)
	# Release low-bin and pre-roll habitat rows before finalization
	del low_bins
	del habitat_tiles
	# Release rolled occurrence arrays immediately after fallback extraction
	del occurrence_rolled
	# Build higher-rank median estimates from rolled elevation bins
	high_medians = medians_from_elevation_bins(elevation_bins)
	# Merge exact low-rank medians over derived higher-rank medians
	elevation_medians = (
		# Start from higher-rank medians derived from profile bins
		high_medians
		# Full join keeps taxa that exist only in low exact or only in high derived sets
		.join(low_medians, on='gbif_id', how='full', suffix='_low')
		.select(
			# Normalize joined id columns back to one gbif_id key
			pl.coalesce([pl.col('gbif_id'), pl.col('gbif_id_low')]).alias('gbif_id'),
			# Prefer exact low-rank median when present, otherwise keep derived high-rank median
			pl.coalesce([pl.col('elevation_low'), pl.col('elevation')]).cast(pl.Int32).alias('elevation'),
		)
	)
	# Release temporary median and parentage frames
	del high_medians
	del low_medians
	del parentage
	# Attach center coordinates to rolled habitat tile counts
	habitat_points = finalize_habitat_maps(habitat_rolled)
	# Release rolled tile rows immediately after finalization
	del habitat_rolled
	# Return habitat points, raw fallback arrays, elevation profile bins, and elevation medians
	return habitat_points, raw_fallback_arrays, elevation_bins, elevation_medians

# Cluster one taxon's habitat points with weighted FAISS and weighted neighborhood ranking
def cluster_points_weighted(x_coords: list, y_coords: list, weights: list) -> list:
	# Build point matrix from lng/lat lists
	points = np.column_stack((np.array(x_coords, dtype=np.float32), np.array(y_coords, dtype=np.float32)))
	# Return all points directly for tiny taxa
	if len(points) <= 3: return points.tolist()
	# Track number of physical vectors
	n_vectors = len(points)
	# Track effective sample count from habitat weights
	effective_n = int(round(float(np.sum(weights))))
	# Keep minimum points-per-centroid aligned with legacy policy
	min_points = max(1, min(n_vectors // 20, 100))
	# Keep adaptive cluster count aligned with legacy policy
	k = 3 if effective_n < 100 else 4 if effective_n < 1000 else 5 if effective_n < 6000 else min(6 + (effective_n - 6000) // 5000, 10)
	# Cap cluster count by available vectors
	k = max(3, min(k, n_vectors))
	# Initialize FAISS kmeans model
	kmeans = faiss.Kmeans(FAISS_DIMS, k, niter=max(5, min(k * 2, 20)), verbose=False, min_points_per_centroid=min_points)
	# Train weighted model on habitat centers
	kmeans.train(points, weights=np.array(weights, dtype=np.float32))
	# Build exact nearest-neighbor index for projection to real points
	index = faiss.IndexFlatL2(FAISS_DIMS)
	# Add points to nearest-neighbor index
	index.add(points)
	# Fast path for k=3 policy
	if k == 3:
		# Project centroids to nearest real points
		_, idx = index.search(kmeans.centroids, 1)
		# Return projected points without Python-side dedupe
		# Dedupe is performed once in DuckDB during package_data
		return [points[i[0]].tolist() for i in idx]
	# Compute cluster neighborhoods
	lims, _, neighbors = index.range_search(kmeans.centroids, FAISS_RANGE_RADIUS)
	# Convert neighborhoods to weighted cluster scores
	cluster_sizes = np.zeros(len(kmeans.centroids), dtype=np.float64)
	# Build weights array once for neighborhood scoring
	weights_array = np.array(weights, dtype=np.float32)
	# Aggregate occurrence weights per centroid neighborhood
	for i in range(len(kmeans.centroids)):
		# Resolve neighborhood index range for current centroid
		start, end = lims[i], lims[i + 1]
		# Sum habitat occurrence weights for current neighborhood
		cluster_sizes[i] = float(weights_array[neighbors[start:end]].sum())
	# Keep clusters above lower-quartile size threshold
	size_threshold = np.percentile(cluster_sizes, 25)
	# Select large clusters using threshold
	large_clusters = np.where(cluster_sizes >= size_threshold)[0]
	# Fall back to largest clusters when threshold leaves too few
	if len(large_clusters) < 3:
		# Keep top clusters by size
		top_clusters = np.argsort(cluster_sizes)[-min(3, len(cluster_sizes)):]
	# Apply diversity-aware selection otherwise
	else:
		# Sort large clusters by descending size
		sorted_large = large_clusters[np.argsort(cluster_sizes[large_clusters])[::-1]]
		# Seed selection with largest cluster
		selected = [sorted_large[0]]
		# Add diverse clusters until selection is full
		for cluster_idx in sorted_large[1:]:
			# Stop when three clusters are selected
			if len(selected) >= 3: break
			# Keep candidate when sufficiently far from selected centroids
			if np.min(np.linalg.norm(kmeans.centroids[cluster_idx] - kmeans.centroids[selected], axis=1)) >= FAISS_DIVERSITY_DISTANCE: selected.append(cluster_idx)
		# Backfill remaining slots if diversity filter left gaps
		if len(selected) < 3:
			# Iterate candidates in deterministic order
			for cluster_idx in sorted_large:
				# Stop once selection is full
				if len(selected) >= 3: break
				# Add candidate not already selected
				if cluster_idx not in selected: selected.append(cluster_idx)
		# Freeze selected cluster ids
		top_clusters = np.array(selected)
	# Select top centroid vectors
	top_centroids = kmeans.centroids[top_clusters]
	# Project selected centroids to nearest real points
	_, idx = index.search(top_centroids, 1)
	# Return projected points without Python-side dedupe
	# Dedupe is performed once in DuckDB during package_data
	return [points[i[0]].tolist() for i in idx]

# Cluster one taxon's raw occurrence arrays with legacy unweighted ranking
def cluster_points_raw(x_coords: list, y_coords: list) -> list:
	# Build point matrix from lng/lat lists
	points = np.column_stack((np.array(x_coords, dtype=np.float32), np.array(y_coords, dtype=np.float32)))
	# Return all points directly for tiny taxa
	if len(points) <= 3: return points.tolist()
	# Track number of physical vectors
	n_vectors = len(points)
	# Keep minimum points-per-centroid aligned with legacy policy
	min_points = max(1, min(n_vectors // 20, 100))
	# Keep adaptive cluster count aligned with legacy policy
	k = 3 if n_vectors < 100 else 4 if n_vectors < 1000 else 5 if n_vectors < 6000 else min(6 + (n_vectors - 6000) // 5000, 10)
	# Cap cluster count by available vectors
	k = max(3, min(k, n_vectors))
	# Initialize FAISS kmeans model
	kmeans = faiss.Kmeans(FAISS_DIMS, k, niter=max(5, min(k * 2, 20)), verbose=False, min_points_per_centroid=min_points)
	# Train unweighted model on raw occurrence points
	kmeans.train(points)
	# Build exact nearest-neighbor index for projection to real points
	index = faiss.IndexFlatL2(FAISS_DIMS)
	# Add points to nearest-neighbor index
	index.add(points)
	# Fast path for k=3 policy
	if k == 3:
		# Project centroids to nearest real points
		_, idx = index.search(kmeans.centroids, 1)
		# Return projected points without Python-side dedupe
		# Dedupe is performed once in DuckDB during package_data
		return [points[i[0]].tolist() for i in idx]
	# Compute cluster neighborhoods
	lims, _, _ = index.range_search(kmeans.centroids, FAISS_RANGE_RADIUS)
	# Convert neighborhoods to unweighted cluster scores
	cluster_sizes = np.diff(lims)
	# Keep clusters above lower-quartile size threshold
	size_threshold = np.percentile(cluster_sizes, 25)
	# Select large clusters using threshold
	large_clusters = np.where(cluster_sizes >= size_threshold)[0]
	# Fall back to largest clusters when threshold leaves too few
	if len(large_clusters) < 3:
		# Keep top clusters by size
		top_clusters = np.argsort(cluster_sizes)[-min(3, len(cluster_sizes)):]
	# Apply diversity-aware selection otherwise
	else:
		# Sort large clusters by descending size
		sorted_large = large_clusters[np.argsort(cluster_sizes[large_clusters])[::-1]]
		# Seed selection with largest cluster
		selected = [sorted_large[0]]
		# Add diverse clusters until selection is full
		for cluster_idx in sorted_large[1:]:
			# Stop when three clusters are selected
			if len(selected) >= 3: break
			# Keep candidate when sufficiently far from selected centroids
			if np.min(np.linalg.norm(kmeans.centroids[cluster_idx] - kmeans.centroids[selected], axis=1)) >= FAISS_DIVERSITY_DISTANCE: selected.append(cluster_idx)
		# Backfill remaining slots if diversity filter left gaps
		if len(selected) < 3:
			# Iterate candidates in deterministic order
			for cluster_idx in sorted_large:
				# Stop once selection is full
				if len(selected) >= 3: break
				# Add candidate not already selected
				if cluster_idx not in selected: selected.append(cluster_idx)
		# Freeze selected cluster ids
		top_clusters = np.array(selected)
	# Select top centroid vectors
	top_centroids = kmeans.centroids[top_clusters]
	# Project selected centroids to nearest real points
	_, idx = index.search(top_centroids, 1)
	# Return projected points without Python-side dedupe
	# Dedupe is performed once in DuckDB during package_data
	return [points[i[0]].tolist() for i in idx]

# Cluster one row tuple inside a worker process
def cluster_worker(record: tuple) -> dict:
	# Unpack worker input tuple
	mode, gbif_id, x_coords, y_coords, weights = record
	# Dispatch to weighted or raw clustering path
	if mode == 'weighted': centroids = cluster_points_weighted(x_coords, y_coords, weights)
	# Cluster low-tile fallback taxa using raw occurrence path
	else: centroids = cluster_points_raw(x_coords, y_coords)
	# Return clustered row payload
	return {'gbif_id': int(gbif_id), 'centroids': centroids}

# Compute mixed centroid outputs from staged habitat and raw fallback parquets
def find_clusters_from_parquet(habitat_path: str, raw_fallback_path: str) -> pl.DataFrame:
	# Announce centroid clustering stage
	print(f"IMPORT : Clustering habitat centroids")
	# Load only clustering columns from staged habitat parquet
	habitat = (
		pl.scan_parquet(habitat_path)
		.select('gbif_id', 'center_lng', 'center_lat', 'count')
		.collect(engine='streaming')
	)
	# Build per-taxon habitat center arrays with weights
	arrays = (
		habitat
		.group_by('gbif_id')
		.agg(
			pl.col('center_lng').cast(pl.Float32).alias('x'),
			pl.col('center_lat').cast(pl.Float32).alias('y'),
			pl.col('count').cast(pl.Float32).alias('w'),
		)
	)
	# Split weighted taxa at tile threshold
	weighted_arrays = arrays.filter(pl.col('x').list.len() >= HABITAT_TILE_THRESHOLD)
	# Load low-tile raw fallback arrays from staged parquet
	raw_fallback_arrays = (
		pl.scan_parquet(raw_fallback_path)
		.select('gbif_id', 'x', 'y')
		.collect(engine='streaming')
	)
	# Release raw habitat rows and unsplit arrays before worker spawn
	del habitat
	del arrays
	# Force a collection cycle before spawning workers
	gc.collect()
	# Track weighted and raw fallback record counts for logs
	weighted_count = len(weighted_arrays)
	# Track raw fallback taxon count for logs
	fallback_count = len(raw_fallback_arrays)
	# Track total taxa prepared for clustering
	total_records = weighted_count + fallback_count
	# Log split counts for weighted and fallback modes
	print(f"IMPORT : Prepared {weighted_count:,} weighted taxa and {fallback_count:,} raw fallback taxa")
	# Stream worker input records instead of materializing one huge Python list
	def record_iter():
		# Yield weighted clustering records first
		for row in weighted_arrays.iter_rows(named=True):
			# Emit weighted payload expected by worker function
			yield ('weighted', int(row['gbif_id']), row['x'], row['y'], row['w'])
		# Yield raw fallback clustering records second
		for row in raw_fallback_arrays.iter_rows(named=True):
			# Emit raw payload expected by worker function
			yield ('raw', int(row['gbif_id']), row['x'], row['y'], None)
	# Choose worker count from CPU capacity (same pattern as high-throughput importer stages)
	workers = max(1, (os.cpu_count() or 2) - 1)
	# Cap Windows worker fan-out to reduce CreateProcess WinError 8 under high memory pressure
	if os.name == 'nt': workers = min(workers, 5)
	# Log worker pool settings
	print(f"IMPORT : Running centroid clustering with {workers} workers")
	# Start clustering timer
	t0 = datetime.now(timezone.utc)
	# Hold clustered rows
	rows = []
	# Open worker pool for parallel clustering
	with mp.Pool(processes=workers) as pool:
		# Iterate results as workers complete tasks
		for idx, row in enumerate(pool.imap_unordered(cluster_worker, record_iter(), chunksize=64), start=1):
			# Append clustered row
			rows.append(row)
			# Emit periodic single-line progress updates
			if idx % 5000 == 0: print(f"\rIMPORT : Clustered {idx:,}/{total_records:,} taxa", end="", flush=True)
	# Release grouped arrays once worker consumption is complete
	del weighted_arrays
	del raw_fallback_arrays
	# Build clustered output frame
	centroids = pl.DataFrame(rows).sort('gbif_id')
	# Compute elapsed seconds for logs
	elapsed = (datetime.now(timezone.utc) - t0).total_seconds()
	# Log clustering completion on the same carriage-return line as progress updates
	print(f"\rIMPORT : Clustered {len(centroids):,} taxa in {elapsed:.1f}s ({len(centroids)/max(0.001, elapsed):.1f} taxa/s)")
	# Return clustered centroids frame
	return centroids

# Package habitat, centroid, and elevation outputs into release geo parquet artifact from staged parquet files
def package_data_from_parquet(release: dict, habitat_path: str, centroids_path: str, elevation_bins_path: str, elevation_medians_path: str):
	# Announce packaging stage
	print(f"IMPORT : Packaging geo artifact")
	# Resolve target release directory
	release_dir = os.path.join(RELEASES_DIR, release.get('version'))
	# Build date-stamped geo artifact name
	filename = f"geo.{datetime.now(timezone.utc).strftime('%Y%m%d')}.parquet"
	# Resolve final output parquet path
	output_path = storage.parquet_url(os.path.join(release_dir, filename))
	# Resolve temporary directory for partitioned packaging output
	parts_dir = os.path.join(TMP_DIR, f"geo_package_parts_{release.get('version')}")
	# Normalize input parquet paths for DuckDB SQL literals
	habitat_path_sql = habitat_path.replace('\\', '/')
	# Normalize centroid parquet path for DuckDB SQL literals
	centroids_path_sql = centroids_path.replace('\\', '/')
	# Normalize elevation-bin parquet path for DuckDB SQL literals
	elevation_bins_path_sql = elevation_bins_path.replace('\\', '/')
	# Normalize elevation-median parquet path for DuckDB SQL literals
	elevation_medians_path_sql = elevation_medians_path.replace('\\', '/')
	# Ensure packaging part directory exists before writing partition outputs
	os.makedirs(parts_dir, exist_ok=True)
	# Remove stale partition files from earlier interrupted runs
	for file in os.listdir(parts_dir):
		# Skip non-parquet files in part directory
		if not file.endswith('.parquet'): continue
		# Remove stale partition output before new run
		os.remove(os.path.join(parts_dir, file))
	# Hold normalized partition parquet paths for final merge
	part_paths = []
	# Open in-memory DuckDB for JSON packaging
	with duckdb.connect(':memory:') as db:
		# Route temporary spill files to canopy temp directory
		db.execute(f"SET temp_directory = '{TMP_DIR}'")
		# Configure DuckDB S3 settings when writing final geo artifact to object storage
		if storage.is_s3(): storage.configure_duckdb(db)
		# Allow DuckDB to optimize grouping without preserving insertion order
		db.execute("SET preserve_insertion_order = false")
		# Keep large Arrow string buffers enabled for wide JSON payload safety
		db.execute('SET arrow_large_buffer_size=true')
		# Process packaging in deterministic hash partitions to cap peak memory
		for part in range(PACKAGE_PARTITIONS):
			# Resolve output parquet path for current packaging partition
			part_path = os.path.join(parts_dir, f"part_{part:02d}.parquet").replace('\\', '/')
			# Track current partition path for final merge
			part_paths.append(part_path)
			# Build one partition of geo payload with the same variant-A semantics
			db.execute(f"""
				COPY (
					WITH habitat_part AS (
						SELECT
							gbif_id,
							array_agg(
								struct_pack(lng := center_lng, lat := center_lat, occ := count)
								ORDER BY CASE WHEN count = 1 THEN 1 ELSE 0 END, count DESC
							)::JSON AS habitat,
							max(count) AS max,
							avg(count) AS avg
						FROM read_parquet('{habitat_path_sql}')
						WHERE abs(hash(gbif_id)) % {PACKAGE_PARTITIONS} = {part}
						GROUP BY gbif_id
					),
					centroid_clean AS (
						SELECT
							gbif_id,
							list_distinct(
								list_transform(
									centroids,
									coord -> [coord[1]::DECIMAL(8,4), coord[2]::DECIMAL(8,4)]
								)
							)::JSON AS centroids
						FROM read_parquet('{centroids_path_sql}')
						WHERE abs(hash(gbif_id)) % {PACKAGE_PARTITIONS} = {part}
					),
					elevation_profile_clean AS (
						SELECT
							gbif_id,
							array_agg(struct_pack(elevation := elevation_bin, occ := bin_count) ORDER BY elevation_bin)::JSON AS elevation_profile
						FROM read_parquet('{elevation_bins_path_sql}')
						WHERE abs(hash(gbif_id)) % {PACKAGE_PARTITIONS} = {part}
						GROUP BY gbif_id
					),
					elevation_medians_clean AS (
						SELECT
							gbif_id,
							cast(elevation AS SMALLINT) AS elevation
						FROM read_parquet('{elevation_medians_path_sql}')
						WHERE abs(hash(gbif_id)) % {PACKAGE_PARTITIONS} = {part}
					)
					SELECT
						h.gbif_id,
						h.habitat,
						h.max,
						h.avg,
						c.centroids,
						m.elevation,
						ep.elevation_profile
					FROM habitat_part h
					LEFT JOIN centroid_clean c ON h.gbif_id = c.gbif_id
					LEFT JOIN elevation_profile_clean ep ON h.gbif_id = ep.gbif_id
					LEFT JOIN elevation_medians_clean m ON h.gbif_id = m.gbif_id
				) TO '{part_path}' (FORMAT PARQUET, COMPRESSION ZSTD)
			""")
			# Emit periodic single-line packaging progress updates
			print(f"\rIMPORT : Packaged geo partition {part + 1}/{PACKAGE_PARTITIONS}", end="", flush=True)
		# Print newline after carriage-return partition updates
		print()
		# Merge all partition outputs into final geo artifact
		db.execute(f"COPY (SELECT * FROM read_parquet('{os.path.join(parts_dir, '*.parquet').replace('\\', '/')}')) TO '{output_path}' (FORMAT PARQUET, COMPRESSION ZSTD)")
	# Remove partition outputs after final merge succeeds
	for path in part_paths:
		# Delete partition file when present
		if os.path.isfile(path): os.remove(path)
	# Log saved geo artifact path
	print(f"IMPORT : Saved table to {release_dir}/{filename}")
	# Attach geo filename to release manifest payload
	release['geo'] = filename
	# Persist release manifest with new geo artifact reference
	storage.write_json(os.path.join(release_dir, 'manifest.json'), release)
