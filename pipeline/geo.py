# Keep heavy geospatial computation isolated from the core taxonomy fusion stage

# Load filesystem and manifest serialization helpers
import os, json
# Load UTC timestamp helpers for deterministic geo artifact naming
from datetime import datetime, timezone
# Load canopy settings and path constants used by geo stage
from .. import settings, TMP_DIR, RELEASES_DIR, GEO_DIR
# Load release and file helpers for stage orchestration
from ..utils.filehandlers import get_latest_release, get_file
# Load occurrence update pipeline that maintains rolling occurrence parquet
from ..datasets.occurrences import update_occurrences
# Load DuckDB for spatial SQL processing
import duckdb
# Load Arrow chunk arrays for FAISS UDF bridge
import pyarrow as pa
# Load NumPy for cluster math and distance checks
import numpy as np
# Load FAISS for centroid clustering on large occurrence clouds
import faiss

# Compute geospatial enrichment for the selected release
async def compute_geospatial(release):
	# Announce start of geo stage in operator logs
	print(f"IMPORT : ############### Computing Geospatial Data ###############")
	# Fall back to latest release when geo is run as a standalone step
	if not release:
		# Load most recent release manifest from canopy releases dir
		release = get_latest_release()
		# Abort when no release is available to attach geo output to
		if not release:
			# Explain missing prerequisite release state
			print(f"IMPORT : No release candidate found, aborting")
			# Stop geo stage because there is no valid target release
			return
		# Log fallback release selection for traceability
		else: print(f"IMPORT : No release provided, falling back on latest staging release {release['version']}")
	# Check whether this release already has a geo artifact
	computed_file = get_file('geo', os.path.join(RELEASES_DIR, release.get('version')))
	# Skip recomputation unless force mode was requested
	if computed_file and not settings.FORCE:
		# Log skip decision and how to override it
		print(f"IMPORT : Release {release.get('version')} already has a geo file {computed_file}, use -f to overrride")
		# Stop because geo output already exists for this release
		return
	# Refresh rolling occurrence dataset before computing release geo artifact
	await update_occurrences()
	# Guard against missing occurrence baseline after update attempt
	if not os.path.isfile(os.path.join(GEO_DIR, 'occurrences.parquet')):
		# Log missing prerequisite dataset and skip geo stage
		print('IMPORT : No occurrences.parquet available, skipping geo step')
		# Stop because no occurrence points are available to cluster
		return
	# Run geo build in one in-memory DuckDB connection
	with duckdb.connect(':memory:') as db:
		# Load occurrence points table and spatial extension
		load_data(release, db)
		# Build per-taxon habitat map buckets
		build_habitat_maps(release, db)
		# Compute representative cluster centroids per taxon
		find_clusters(release, db)
		# Merge habitat, centroids, and elevation into final geo artifact
		package_data(release, db)

# Load and filter occurrence data into DuckDB working table
def load_data(release: dict, db: duckdb.DuckDBPyConnection):
	# Route temporary DuckDB spill files to canopy temp directory
	db.execute(f"SET temp_directory = '{ TMP_DIR }'")
	# Open rolling occurrence parquet as a DuckDB relation
	occurrence_parquet = db.read_parquet(os.path.join(GEO_DIR, 'occurrences.parquet'))
	# Announce potentially expensive table materialization
	print("IMPORT : Opened occurrence parquet, loading into table - will take a minute...")
	# Build filtered occurrence table for downstream habitat and clustering SQL
	db.execute(f"""
		-- Load spatial extension once for geometry functions
		INSTALL spatial;
		LOAD spatial;
		-- Keep only fields needed by downstream geo steps
		CREATE TABLE occurrences AS SELECT taxon, location, elevation FROM occurrence_parquet
		-- Drop rows flagged with spatial issues and known null-island sentinels
		WHERE NOT spatial_issue AND NOT ST_Equals(location, ST_Point(0, 0)) AND NOT ST_Equals(location, ST_Point(1, 1))
		-- Keep debug runs bounded for faster local iteration
		{'LIMIT 10000000' if settings.DEBUG else ''};
	""")
	# Count retained rows for operator visibility
	initial_count = db.execute("SELECT COUNT(*) FROM occurrences").fetchone()[0]
	# Log loaded occurrence row count after filtering
	print(f"IMPORT : Loaded {initial_count:,} occurrences")
	# Keep RTREE index disabled because prior benchmarks showed slower end-to-end runtime
	# db.execute(f"""CREATE INDEX point_idx ON occurrences USING RTREE (location);""")
	# Keep reference log in case we revisit index experiments later
	# print(f"IMPORT : Added R-TREE index")

# Build coarse habitat envelope points grouped by taxon and quadkey tile
def build_habitat_maps(release: dict, db: duckdb.DuckDBPyConnection):
	# Announce habitat aggregation step duration expectations
	print(f"IMPORT : Computing initial habitat data, this will take about 5 mins...")
	# Aggregate points into tile-level centers and counts for each taxon
	db.execute(f"""
		CREATE TEMP TABLE habitat_points AS SELECT
			taxon AS gbif_id,
			-- Keep quadkey for rollup grouping in next stage
			ST_QuadKey(location, 10) AS qk,
			-- Union all points into multipoint then take centroid — avoids POINT EMPTY
			-- that ST_Centroid(ST_Envelope_Agg(...)) produces on zero-area single-point tiles
			ST_ReducePrecision(ST_Centroid(ST_Union_Agg(location)),0.001) as center_point,
			COUNT(*) as count
		-- QuadKey level 10 balances detail and payload size for client maps
		FROM occurrences GROUP BY taxon, ST_QuadKey(location, 10);
	""")
	# Log number of generated habitat buckets
	print(f"IMPORT : Computed {db.execute("SELECT COUNT(*) FROM habitat_points").fetchone()[0]:,} unique habitat counts")
	# Show sample habitat rows in verbose mode for debugging
	if settings.VERBOSE: db.sql('SELECT * FROM habitat_points').show(max_rows=40)


# Cluster each species' occurrence cloud into representative centroids
def find_clusters(release: dict, db: duckdb.DuckDBPyConnection):
	# Announce species-level aggregation before FAISS step
	print(f"IMPORT : Aggregating geo points per species...")
	# Build one row per species with coordinate arrays for UDF clustering
	db.execute(f"""
		CREATE TEMP TABLE species_arrays AS SELECT
			taxon,
			array_agg(ST_X(location)::FLOAT) AS x,
			array_agg(ST_Y(location)::FLOAT) AS y,
			median(elevation) AS elevation
		FROM occurrences GROUP BY taxon;
	""")
	# Log species row count prepared for FAISS clustering
	print(f"IMPORT : Consolidated points into {db.execute("SELECT COUNT(*) FROM species_arrays").fetchone()[0]:,} rows")
	# Optionally print species arrays preview during debug sessions
	if settings.VERBOSE: db.sql('SELECT * FROM species_arrays;').show(max_rows=40)
	# Build Arrow UDF input/output type for FLOAT[] coordinate arrays
	float32_array = db.list_type(db.sqltype('FLOAT'))
	# Register Python FAISS clustering UDF in current DuckDB connection
	db.create_function('faiss_cluster', faiss_clustering, [float32_array, float32_array], db.list_type(float32_array), type='arrow')
	# Confirm FAISS UDF registration in logs
	print(f"IMPORT : Registered FAISS UDF")
	# Execute clustering and normalize output into JSON [lng, lat] coordinate pairs
	db.execute(f"""
		-- Add centroid output column once per run
		ALTER TABLE species_arrays ADD COLUMN IF NOT EXISTS centroids JSON;
		-- Cluster each taxon and deduplicate repeated centroid coordinates
		UPDATE species_arrays SET centroids = list_distinct(list_transform(faiss_cluster(x, y), coord -> [coord[1]::DECIMAL(8,4), coord[2]::DECIMAL(8,4)]))::JSON WHERE x IS NOT NULL;
	""")
	# Log clustered row count to monitor UDF coverage
	print(f"\nIMPORT : Clustered {db.execute("SELECT COUNT(*) FROM species_arrays WHERE centroids IS NOT NULL").fetchone()[0]:,} rows")
	# Optionally show clustered rows for verbose diagnostics
	if settings.VERBOSE: db.sql("SELECT * FROM species_arrays").show(max_rows=200)
	# Drop raw coordinate arrays after clustering to release memory
	db.execute(f"""
		-- Remove large intermediate coordinate arrays after centroid extraction
		ALTER TABLE species_arrays DROP COLUMN IF EXISTS x;
		ALTER TABLE species_arrays DROP COLUMN IF EXISTS y;
	""")

# Track clustered species progress across Arrow UDF batches
cluster_count = 0
# Keep FAISS vector dimensionality fixed to [lng, lat]
faiss_dimensions = 2

# Cluster coordinate arrays and return representative occurrence points
def faiss_clustering(x: pa.ChunkedArray, y: pa.ChunkedArray) -> pa.ChunkedArray:
	# Collect centroid output list in Arrow return shape
	results = []
	# Iterate chunk-by-chunk to keep Arrow conversion bounded
	for i in range(len(x.chunks)):
		# Convert longitude chunk to NumPy for FAISS operations
		xnp = x.chunks[i].to_numpy(zero_copy_only=False)
		# Convert latitude chunk to NumPy for FAISS operations
		ynp = y.chunks[i].to_numpy(zero_copy_only=False)
		# Iterate each species row in current Arrow chunk
		for j in range(len(xnp)):
			# Extract longitude array for current species
			x_coords = xnp[j]
			# Extract latitude array for current species
			y_coords = ynp[j]
			# Build [lng, lat] point matrix for current species
			points = np.column_stack((x_coords, y_coords))
			# Return all points directly for tiny species sets
			if len(points) <= 3: results.append(points.tolist())
			# Run FAISS clustering for larger species sets
			else:
				# Cache point count for cluster policy logic
				n = len(points)
				# Keep minimum points-per-centroid bounded for stability
				min_points = max(1, min(n // 20, 100))
				# Scale cluster count with sample size while capping at 10
				cluster_setting = 3 if n < 100 else 4 if n < 1000 else 5 if n < 6000 else min(6 + (n - 6000) // 5000, 10)
				# Initialize FAISS kmeans with adaptive iteration count
				kmeans = faiss.Kmeans(faiss_dimensions, cluster_setting, niter=max(5, min(cluster_setting * 2, 20)), verbose=False, min_points_per_centroid=min_points)
				# Train centroid model on current species points
				kmeans.train(points)
				# Build exact L2 index to map centroids back to real points
				index = faiss.IndexFlatL2(faiss_dimensions)
				# Load all source points into nearest-neighbor index
				index.add(points)
				# Skip ranking when requested cluster count already matches desired output size
				if cluster_setting == 3:
					# Find nearest real point for each centroid
					_, indices = index.search(kmeans.centroids, 1)
					# Project centroid matches back to original occurrence coordinates
					nearest_points = [points[idx[0]].tolist() for idx in indices]
				# Rank and select diverse centroids for dense species clouds
				else:
					# Estimate cluster neighborhood size within ~555km radius
					lims, D, I = index.range_search(kmeans.centroids, 5.0)
					# Convert range-search offsets to per-centroid neighborhood counts
					cluster_sizes = np.diff(lims)
					# Use lower quartile as outlier cutoff for tiny clusters
					size_threshold = np.percentile(cluster_sizes, 25)
					# Keep clusters above minimum size threshold
					large_clusters = np.where(cluster_sizes >= size_threshold)[0]
					# Fall back to top-by-size when too few large clusters remain
					if len(large_clusters) < 3:
						# Limit to available centroid count
						top_count = min(3, len(cluster_sizes))
						# Select largest clusters by neighborhood size
						top_clusters = np.argsort(cluster_sizes)[-top_count:]
					# Prefer geographically diverse large clusters when possible
					else:
						# Sort large clusters by descending neighborhood size
						sorted_large = large_clusters[np.argsort(cluster_sizes[large_clusters])[::-1]]
						# Seed selection with strongest cluster
						selected = [sorted_large[0]]
						# Add additional clusters that are sufficiently far apart
						for cluster_idx in sorted_large[1:]:
							# Stop when we already selected three candidates
							if len(selected) >= 3: break
							# Accept cluster when its centroid is at least ~1000km away
							if np.min(np.linalg.norm(kmeans.centroids[cluster_idx] - kmeans.centroids[selected], axis=1)) >= 9.0: selected.append(cluster_idx)
						# Backfill remaining slots from sorted pool if diversity filter left gaps
						if len(selected) < 3:
							# Iterate sorted candidates for deterministic fill order
							for cluster_idx in sorted_large:
								# Stop once we filled required output size
								if len(selected) >= 3: break
								# Add cluster when it was not selected yet
								if cluster_idx not in selected: selected.append(cluster_idx)
						# Freeze selected cluster indices as NumPy array for indexing
						top_clusters = np.array(selected)
					# Get selected centroid vectors for nearest-point projection
					top_centroids = kmeans.centroids[top_clusters]
					# Find nearest real occurrence point per selected centroid
					_, indices = index.search(top_centroids, 1)
					# Convert nearest-point indices into coordinate lists
					nearest_points = [points[idx[0]].tolist() for idx in indices]
				# Append selected representative points for current species
				results.append(nearest_points)
	# Use global progress counter for streaming batch feedback
	global cluster_count
	# Increment by Arrow batch size used for this UDF pipeline
	cluster_count += 2048
	# Print live progress line without adding new lines each batch
	print(f"\rIMPORT : Clustered {cluster_count:,} species' occurrences", end="")
	# Return Arrow-compatible list of clustered coordinate arrays
	return results

# Assemble final geo table and persist release-specific geo artifact
def package_data(release: dict, db: duckdb.DuckDBPyConnection):
	# Build base geo table with habitat envelope payloads and summary stats
	db.execute(f"""
		CREATE TABLE geo AS
		-- Cast aggregate payload to JSON to keep parquet export predictable
		SELECT gbif_id, array_agg(struct_pack(
			-- Keep globe.gl coordinate field names expected by frontend
			lng := ST_X(center_point),
			lat := ST_Y(center_point),
			occ := count
		))::JSON AS habitat,
		max(count) AS max,
		avg(count) as avg
		FROM habitat_points GROUP BY gbif_id;
		-- Drop temporary habitat table after packaging
		DROP TABLE habitat_points;
	""")
	# Log number of packaged geo habitat rows
	print(f"IMPORT : Built geo table with {db.execute("SELECT COUNT(*) FROM geo").fetchone()[0]:,} habitat maps")
	# Show packaged rows in verbose mode for diagnostics
	if settings.VERBOSE: db.sql("SELECT * FROM geo").show(max_rows=40)
	# Attach centroid and elevation outputs from species arrays table
	db.execute(f"""
		-- Add output columns once per table build
		ALTER TABLE geo ADD COLUMN IF NOT EXISTS centroids JSON;
		ALTER TABLE geo ADD COLUMN IF NOT EXISTS elevation SMALLINT;
		-- Merge centroid and elevation values by gbif taxon id
		UPDATE geo SET
			centroids = species_arrays.centroids,
			elevation = species_arrays.elevation,
		FROM species_arrays WHERE geo.gbif_id = species_arrays.taxon;
		-- Drop temporary species arrays table after merge
		DROP TABLE species_arrays;
	""")
	# Confirm centroid/elevation merge completion
	print(f"IMPORT : Added centroids & median elevation")
	# Show merged geo rows in verbose mode
	if settings.VERBOSE: db.sql("SELECT * FROM geo").show(max_rows=40)
	# Resolve target release directory path
	release_dir = os.path.join(RELEASES_DIR, release.get('version'))
	# Generate date-stamped geo artifact filename
	filename = f'geo.{datetime.now(timezone.utc).strftime("%Y%m%d")}.parquet'
	# Write final geo parquet artifact to current release dir
	db.sql(f"SELECT * FROM geo;").write_parquet(f'{release_dir}/{filename}')
	# Log saved geo artifact path
	print(f"IMPORT : Saved table to {release_dir}/{filename}")
	# Attach geo artifact filename to release manifest
	release['geo'] = filename
	# Persist updated release manifest including geo artifact metadata
	with open(os.path.join(RELEASES_DIR, f"{release['version']}/manifest.json"), 'w') as f: json.dump(release, f, indent=4)

# We eventually want to open source this geo approach as GSIFT
# It combines GBIF occurrences with map-like classification surfaces
# Example downstream layers include life zones and soil preference maps
