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
	# Guard against missing occurrence parquet after update
	if not os.path.isfile(os.path.join(GEO_DIR, 'occurrences.parquet')):
		# Log missing prerequisite dataset
		print('IMPORT : No occurrences.parquet available, skipping geo step')
		# Stop stage due to missing input
		return
	# Build taxon habitat point table from accepted and rolled occurrence arrays
	habitat_points = build_habitat_for_release(release)
	# Build representative centroid points per taxon
	clustered_centroids = find_clusters(habitat_points)
	# Package habitat and centroid outputs into final geo artifact
	package_data(release, habitat_points, clustered_centroids)

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

# Roll up species occurrences to parent taxa by concatenating children's arrays
def rollup_to_parents(compact: pl.DataFrame, release: dict) -> pl.DataFrame:
	# Announce rollup stage
	print(f"IMPORT : Rolling up occurrences to parent taxa")
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
	print(f"IMPORT : Building habitat maps")
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

# Build habitat points for a release and free intermediate occurrence arrays inside stage helper
def build_habitat_for_release(release: dict) -> pl.DataFrame:
	# Build species-level occurrence arrays from rolling geoparquet
	occurrence_compact = load_occurrences(release)
	# Build parent-level occurrence arrays from accepted taxonomy graph
	occurrence_rolled = rollup_to_parents(occurrence_compact, release)
	# Release compact arrays immediately after rollup stage
	del occurrence_compact
	# Build taxon habitat point table from rolled occurrence arrays
	habitat_points = build_habitat_maps(occurrence_rolled)
	# Release rolled arrays immediately after habitat stage
	del occurrence_rolled
	# Return habitat points for downstream clustering
	return habitat_points

# Cluster one taxon's habitat points with weighted FAISS and legacy selection policy
def cluster_points(x_coords: list, y_coords: list, weights: list) -> list:
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
	# Compute cluster neighborhood sizes
	lims, _, _ = index.range_search(kmeans.centroids, FAISS_RANGE_RADIUS)
	# Convert range-search offsets to cluster size counts
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
	gbif_id, x_coords, y_coords, weights = record
	# Cluster current taxon points
	centroids = cluster_points(x_coords, y_coords, weights)
	# Return clustered row payload
	return {'gbif_id': int(gbif_id), 'centroids': centroids}

# Compute weighted habitat centroids for all taxa
def find_clusters(habitat: pl.DataFrame) -> pl.DataFrame:
	# Announce centroid clustering stage
	print(f"IMPORT : Clustering habitat centroids")
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
	# Log number of taxa prepared for clustering
	print(f"IMPORT : Prepared {len(arrays):,} taxa for FAISS clustering")
	# Build worker input records
	records = [
		(int(row['gbif_id']), row['x'], row['y'], row['w'])
		for row in arrays.iter_rows(named=True)
	]
	# Choose worker count from CPU capacity
	workers = max(1, min(8, (os.cpu_count() or 2) - 1))
	# Log worker pool settings
	print(f"IMPORT : Running centroid clustering with {workers} workers")
	# Start clustering timer
	t0 = datetime.now(timezone.utc)
	# Hold clustered rows
	rows = []
	# Open worker pool for parallel clustering
	with mp.Pool(processes=workers) as pool:
		# Iterate results as workers complete tasks
		for idx, row in enumerate(pool.imap_unordered(cluster_worker, records, chunksize=64), start=1):
			# Append clustered row
			rows.append(row)
			# Emit periodic single-line progress updates
			if idx % 5000 == 0: print(f"\rIMPORT : Clustered {idx:,}/{len(records):,} taxa", end="", flush=True)
	# Print newline after carriage-return progress updates
	print()
	# Build clustered output frame
	centroids = pl.DataFrame(rows).sort('gbif_id')
	# Compute elapsed seconds for logs
	elapsed = (datetime.now(timezone.utc) - t0).total_seconds()
	# Log clustering completion
	print(f"IMPORT : Clustered {len(centroids):,} taxa in {elapsed:.1f}s ({len(centroids)/max(0.001, elapsed):.1f} taxa/s)")
	# Return clustered centroids frame
	return centroids

# Package habitat and centroid outputs into release geo parquet artifact
def package_data(release: dict, habitat: pl.DataFrame, centroids: pl.DataFrame):
	# Announce packaging stage
	print(f"IMPORT : Packaging geo artifact")
	# Resolve target release directory
	release_dir = os.path.join(RELEASES_DIR, release.get('version'))
	# Build date-stamped geo artifact name
	filename = f"geo.{datetime.now(timezone.utc).strftime('%Y%m%d')}.parquet"
	# Open in-memory DuckDB for JSON packaging
	with duckdb.connect(':memory:') as db:
		# Route temporary spill files to canopy temp directory
		db.execute(f"SET temp_directory = '{TMP_DIR}'")
		# Register habitat frame in DuckDB
		db.register('habitat_df', habitat.to_arrow())
		# Register centroid frame in DuckDB
		db.register('centroid_df', centroids.to_arrow())
		# Build geo table with habitat payload and summary metrics
		db.execute("""
			CREATE TABLE geo AS
			SELECT
				gbif_id,
				array_agg(struct_pack(lng := center_lng, lat := center_lat, occ := count))::JSON AS habitat,
				max(count) AS max,
				avg(count) AS avg
			FROM habitat_df
			GROUP BY gbif_id;
		""")
		# Add centroid and elevation columns
		db.execute("""
			ALTER TABLE geo ADD COLUMN IF NOT EXISTS centroids JSON;
			ALTER TABLE geo ADD COLUMN IF NOT EXISTS elevation SMALLINT;
		""")
		# Normalize centroid payload in DuckDB and dedupe once per taxon
		# Match legacy geo.py output shape and precision exactly:
		# list_distinct(list_transform(... coord -> [coord[1]::DECIMAL(8,4), coord[2]::DECIMAL(8,4)]))
		db.execute("""
			CREATE TEMP TABLE centroid_clean AS
			SELECT
				gbif_id,
				list_distinct(
					list_transform(
						centroids,
						coord -> [coord[1]::DECIMAL(8,4), coord[2]::DECIMAL(8,4)]
					)
				)::JSON AS centroids
			FROM centroid_df;
		""")
		# Merge cleaned centroid output into geo table
		db.execute("""
			UPDATE geo SET centroids = centroid_clean.centroids
			FROM centroid_clean
			WHERE geo.gbif_id = centroid_clean.gbif_id;
		""")
		# Write packaged geo parquet artifact
		db.sql("SELECT * FROM geo").write_parquet(f"{release_dir}/{filename}")
	# Log saved geo artifact path
	print(f"IMPORT : Saved table to {release_dir}/{filename}")
	# Attach geo filename to release manifest payload
	release['geo'] = filename
	# Persist release manifest with new geo artifact reference
	with open(os.path.join(release_dir, 'manifest.json'), 'w', encoding='utf-8') as f: json.dump(release, f, indent=4)
