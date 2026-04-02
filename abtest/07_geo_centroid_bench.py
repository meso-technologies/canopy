# Import cli helpers for benchmark controls
import argparse
# Import filesystem helpers for cache paths
import os
# Import json writer for summary artifacts
import json
# Import wall-clock timers
import time
# Import gc for explicit memory cleanup between phases
import gc
# Import threading for peak rss sampling
import threading
# Import process metrics for resource tracking
import psutil
# Import numeric arrays for FAISS input and math
import numpy as np
# Import FAISS clustering backend used in production geo pipeline
import faiss
# Import polars for cache, sampling, and tabular IO
import polars as pl
# Import sys path helpers for script-mode package resolution
import sys
# Resolve canopy package directory from this file path
canopy_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
# Resolve parent directory so `import canopy` works in script mode
canopy_parent_dir = os.path.dirname(canopy_dir)
# Resolve data service root for shared config imports
service_root_dir = os.path.dirname(canopy_parent_dir)
# Add canopy parent directory to python path
if canopy_parent_dir not in sys.path: sys.path.insert(0, canopy_parent_dir)
# Add service root directory for `config.*` imports
if service_root_dir not in sys.path: sys.path.insert(0, service_root_dir)
# Import canopy temp and release directories
from canopy import TMP_DIR, RELEASES_DIR, GEO_DIR
# Import new geo pipeline stages for cache creation
from canopy.pipeline.geo import load_occurrences, rollup_to_parents, build_habitat_maps

# Keep FAISS dimensionality fixed to [lng, lat]
FAISS_DIMS = 2
# Keep clustering diversity radius aligned with production logic
FAISS_RANGE_RADIUS = 5.0
# Keep centroid separation floor aligned with production logic
FAISS_DIVERSITY_DISTANCE = 9.0

# Sample process RSS while a stage is running
class RssSampler:
	# Initialize sampler state
	def __init__(self, interval_seconds: float = 0.2):
		# Store sample interval
		self.interval_seconds = interval_seconds
		# Store process handle for rss reads
		self.process = psutil.Process(os.getpid())
		# Track start rss
		self.start_rss = 0
		# Track peak rss
		self.peak_rss = 0
		# Track running flag
		self.running = False
		# Hold worker thread
		self.thread = None

	# Start sampling loop
	def start(self):
		# Capture start rss
		self.start_rss = self.process.memory_info().rss
		# Seed peak rss
		self.peak_rss = self.start_rss
		# Mark running state
		self.running = True
		# Spawn daemon thread
		self.thread = threading.Thread(target=self._loop, daemon=True)
		# Start worker
		self.thread.start()

	# Stop loop and return stats
	def stop(self):
		# End worker loop
		self.running = False
		# Join worker if present
		if self.thread is not None: self.thread.join()
		# Read final rss
		end_rss = self.process.memory_info().rss
		# Return formatted rss metrics
		return {
			'start_rss_gb': self.start_rss / (1024 ** 3),
			'peak_rss_gb': self.peak_rss / (1024 ** 3),
			'end_rss_gb': end_rss / (1024 ** 3),
			'peak_delta_gb': (self.peak_rss - self.start_rss) / (1024 ** 3),
		}

	# Poll rss until stop flag is set
	def _loop(self):
		# Continue sampling while running
		while self.running:
			# Read current rss
			rss = self.process.memory_info().rss
			# Raise peak when needed
			if rss > self.peak_rss: self.peak_rss = rss
			# Sleep to next sample tick
			time.sleep(self.interval_seconds)

# Build deterministic file paths for cache and benchmark artifacts
def build_paths(release_id: str) -> dict:
	# Build release-specific path token
	token = release_id.replace('/', '_')
	# Return all file paths used by this script
	return {
		'habitat_points': os.path.join(TMP_DIR, f'geo_cache_{token}_habitat_points.parquet'),
		'sample_taxa': os.path.join(TMP_DIR, f'geo_cache_{token}_sample_taxa.parquet'),
		'raw_arrays': os.path.join(TMP_DIR, f'geo_cache_{token}_raw_arrays_sample.parquet'),
		'habitat_arrays': os.path.join(TMP_DIR, f'geo_cache_{token}_habitat_arrays_sample.parquet'),
		'centroids_baseline': os.path.join(TMP_DIR, f'geo_cache_{token}_centroids_baseline.parquet'),
		'centroids_linear': os.path.join(TMP_DIR, f'geo_cache_{token}_centroids_linear.parquet'),
		'centroids_sqrt': os.path.join(TMP_DIR, f'geo_cache_{token}_centroids_sqrt.parquet'),
		'comparison': os.path.join(TMP_DIR, f'geo_cache_{token}_centroid_comparison.json'),
	}

# Create full habitat_points cache once so downstream centroid tests can reuse it
def ensure_habitat_cache(release_id: str, paths: dict, rebuild_cache: bool):
	# Reuse cache when file exists and rebuild was not requested
	if os.path.exists(paths['habitat_points']) and not rebuild_cache:
		# Log cache reuse
		print(f"ABTEST : Reusing habitat cache {paths['habitat_points']}")
		# Stop early when cache is ready
		return
	# Log cache build start
	print('ABTEST : Building habitat cache from geo stages')
	# Build minimal release dict expected by geo helpers
	release = {'version': release_id}
	# Run load stage
	compact = load_occurrences(release)
	# Run parent rollup stage
	rolled = rollup_to_parents(compact, release)
	# Free pre-rollup table before habitat build
	del compact
	# Run habitat aggregation stage
	habitat = build_habitat_maps(rolled)
	# Free rolled table after habitat build
	del rolled
	# Write habitat cache parquet
	habitat.write_parquet(paths['habitat_points'])
	# Log cache completion
	print(f"ABTEST : Wrote habitat cache {paths['habitat_points']}")
	# Release memory after cache write
	del habitat
	gc.collect()

# Build stratified 50k-style sample from habitat stats with deterministic seed
def build_stratified_sample(paths: dict, sample_size: int, seed: int, rebuild_sample: bool) -> pl.DataFrame:
	# Reuse sample cache when available and rebuild not requested
	if os.path.exists(paths['sample_taxa']) and not rebuild_sample:
		# Log sample reuse
		print(f"ABTEST : Reusing sample cache {paths['sample_taxa']}")
		# Return cached sample frame
		return pl.read_parquet(paths['sample_taxa'])
	# Log sample build start
	print('ABTEST : Building stratified sample from habitat stats')
	# Load habitat points lazily for aggregation
	habitat_lf = pl.scan_parquet(paths['habitat_points'])
	# Build per-taxon totals for sampling buckets
	stats = (
		habitat_lf
		.group_by('gbif_id')
		.agg(
			pl.col('count').sum().alias('occ'),
			pl.len().alias('tiles'),
		)
		.with_columns(
			pl.when(pl.col('occ') <= 3).then(pl.lit('b1_le3'))
			.when(pl.col('occ') <= 20).then(pl.lit('b2_4_20'))
			.when(pl.col('occ') <= 200).then(pl.lit('b3_21_200'))
			.when(pl.col('occ') <= 5000).then(pl.lit('b4_201_5000'))
			.otherwise(pl.lit('b5_gt5000'))
			.alias('bucket')
		)
		.collect()
	)
	# Compute per-bucket target count
	bucket_target = max(1, sample_size // 5)
	# Hold selected frames for each bucket
	selected = []
	# Sample each bucket independently to preserve edge-case coverage
	for bucket in ['b1_le3', 'b2_4_20', 'b3_21_200', 'b4_201_5000', 'b5_gt5000']:
		# Keep rows from current bucket
		bucket_df = stats.filter(pl.col('bucket') == bucket)
		# Skip empty buckets
		if len(bucket_df) == 0: continue
		# Compute sample size for current bucket
		n = min(bucket_target, len(bucket_df))
		# Append deterministic bucket sample
		selected.append(bucket_df.sample(n=n, seed=seed, shuffle=True))
	# Merge selected bucket samples
	sample = pl.concat(selected, how='vertical') if selected else pl.DataFrame({'gbif_id': [], 'occ': [], 'tiles': [], 'bucket': []})
	# Fill remaining slots from not-yet-selected pool
	remaining = sample_size - len(sample)
	# Add remainder sample when needed
	if remaining > 0:
		# Build selected taxon set for anti-join
		selected_ids = set(sample['gbif_id'].to_list())
		# Filter stats to rows not already selected
		pool = stats.filter(~pl.col('gbif_id').is_in(selected_ids))
		# Append random remainder from pool
		if len(pool) > 0:
			extra = pool.sample(n=min(remaining, len(pool)), seed=seed + 1, shuffle=True)
			sample = pl.concat([sample, extra], how='vertical')
	# Keep deterministic row order by taxon id
	sample = sample.sort('gbif_id')
	# Persist sample cache for reuse
	sample.write_parquet(paths['sample_taxa'])
	# Log sample summary
	print(f"ABTEST : Sample size {len(sample):,} written to {paths['sample_taxa']}")
	# Return sample frame
	return sample

# Build raw species arrays for baseline clustering only for sampled taxa
def build_raw_arrays_cache(paths: dict, sample: pl.DataFrame, rebuild_inputs: bool):
	# Reuse cache when available and rebuild not requested
	if os.path.exists(paths['raw_arrays']) and not rebuild_inputs:
		# Log cache reuse
		print(f"ABTEST : Reusing raw arrays cache {paths['raw_arrays']}")
		# Return cached frame
		return pl.read_parquet(paths['raw_arrays'])
	# Log cache build start
	print('ABTEST : Building raw x/y arrays for sampled taxa')
	# Convert sampled ids to Python set for filter pushdown
	sampled_ids = set(sample['gbif_id'].to_list())
	# Build baseline input arrays from occurrences parquet
	raw_arrays = (
		pl.scan_parquet(os.path.join(GEO_DIR, 'occurrences.parquet'))
		.select('taxon', 'location', 'elevation', 'spatial_issue')
		.filter(~pl.col('spatial_issue'), pl.col('taxon').is_in(sampled_ids))
		.with_columns(
			pl.col('location').bin.slice(5, 8).bin.reinterpret(dtype=pl.Float64).cast(pl.Float32).alias('x'),
			pl.col('location').bin.slice(13, 8).bin.reinterpret(dtype=pl.Float64).cast(pl.Float32).alias('y'),
		)
		.filter(
			~((pl.col('x') == 0.0) & (pl.col('y') == 0.0)),
			~((pl.col('x') == 1.0) & (pl.col('y') == 1.0)),
		)
		.group_by('taxon')
		.agg(
			pl.col('x'),
			pl.col('y'),
			pl.col('elevation').median().alias('elevation'),
		)
		.rename({'taxon': 'gbif_id'})
		.collect(engine='streaming')
	)
	# Persist raw arrays cache
	raw_arrays.write_parquet(paths['raw_arrays'])
	# Log cache summary
	print(f"ABTEST : Raw arrays rows {len(raw_arrays):,} written to {paths['raw_arrays']}")
	# Return raw arrays frame
	return raw_arrays

# Build habitat center arrays and weights for sampled taxa

def build_habitat_arrays_cache(paths: dict, sample: pl.DataFrame, rebuild_inputs: bool):
	# Reuse cache when available and rebuild not requested
	if os.path.exists(paths['habitat_arrays']) and not rebuild_inputs:
		# Log cache reuse
		print(f"ABTEST : Reusing habitat arrays cache {paths['habitat_arrays']}")
		# Return cached frame
		return pl.read_parquet(paths['habitat_arrays'])
	# Log cache build start
	print('ABTEST : Building habitat center arrays for sampled taxa')
	# Convert sampled ids to Python set for filter pushdown
	sampled_ids = set(sample['gbif_id'].to_list())
	# Build candidate input arrays from habitat points
	habitat_arrays = (
		pl.scan_parquet(paths['habitat_points'])
		.filter(pl.col('gbif_id').is_in(sampled_ids))
		.group_by('gbif_id')
		.agg(
			pl.col('center_lng').cast(pl.Float32).alias('x'),
			pl.col('center_lat').cast(pl.Float32).alias('y'),
			pl.col('count').cast(pl.Float32).alias('w'),
		)
		.collect(engine='streaming')
	)
	# Persist habitat arrays cache
	habitat_arrays.write_parquet(paths['habitat_arrays'])
	# Log cache summary
	print(f"ABTEST : Habitat arrays rows {len(habitat_arrays):,} written to {paths['habitat_arrays']}")
	# Return habitat arrays frame
	return habitat_arrays

# Remove duplicate coordinate pairs while preserving order

def dedupe_points(points: list) -> list:
	# Track seen coordinate keys
	seen = set()
	# Hold deduped output
	out = []
	# Iterate points in original order
	for p in points:
		# Build hashable rounded key for stable dedupe
		key = (round(float(p[0]), 6), round(float(p[1]), 6))
		# Skip already-emitted coordinates
		if key in seen: continue
		# Track key in seen set
		seen.add(key)
		# Append point to output list
		out.append([float(p[0]), float(p[1])])
	# Return deduped points
	return out

# Run production-like FAISS clustering on one taxon with optional sample weights

def cluster_points(x_coords: list, y_coords: list, weights: list | None):
	# Build Nx2 point matrix for FAISS
	points = np.column_stack((np.array(x_coords, dtype=np.float32), np.array(y_coords, dtype=np.float32)))
	# Return direct points for tiny inputs to preserve fallback behavior
	if len(points) <= 3: return dedupe_points(points.tolist())
	# Track physical vector count used for FAISS training
	n_vectors = len(points)
	# Estimate effective sample count for adaptive k policy
	effective_n = int(round(float(np.sum(weights)))) if weights is not None else n_vectors
	# Keep minimum points-per-centroid based on physical vectors
	min_points = max(1, min(n_vectors // 20, 100))
	# Keep adaptive cluster count aligned with production policy using effective sample count
	k = 3 if effective_n < 100 else 4 if effective_n < 1000 else 5 if effective_n < 6000 else min(6 + (effective_n - 6000) // 5000, 10)
	# Cap cluster count to available vectors
	k = max(3, min(k, n_vectors))
	# Initialize FAISS kmeans model
	kmeans = faiss.Kmeans(FAISS_DIMS, k, niter=max(5, min(k * 2, 20)), verbose=False, min_points_per_centroid=min_points)
	# Train with optional weights when provided
	if weights is None: kmeans.train(points)
	# Train weighted model for habitat variants
	else: kmeans.train(points, weights=np.array(weights, dtype=np.float32))
	# Build exact nearest-neighbor index
	index = faiss.IndexFlatL2(FAISS_DIMS)
	# Add source points to NN index
	index.add(points)
	# Use direct centroid-to-point mapping for k=3 policy path
	if k == 3:
		# Find nearest real point for each centroid
		_, idx = index.search(kmeans.centroids, 1)
		# Project nearest points to list format
		nearest = [points[i[0]].tolist() for i in idx]
		# Return deduped projected points
		return dedupe_points(nearest)
	# Compute centroid neighborhoods for large-k ranking
	lims, _, _ = index.range_search(kmeans.centroids, FAISS_RANGE_RADIUS)
	# Convert range-search offsets to cluster sizes
	cluster_sizes = np.diff(lims)
	# Compute lower-quartile size threshold
	size_threshold = np.percentile(cluster_sizes, 25)
	# Keep clusters above threshold
	large_clusters = np.where(cluster_sizes >= size_threshold)[0]
	# Fallback to largest clusters when threshold leaves too few
	if len(large_clusters) < 3:
		# Limit output count to available clusters
		top_count = min(3, len(cluster_sizes))
		# Select largest clusters by size
		top_clusters = np.argsort(cluster_sizes)[-top_count:]
	# Apply diversity-aware selection when enough large clusters exist
	else:
		# Sort large clusters by descending size
		sorted_large = large_clusters[np.argsort(cluster_sizes[large_clusters])[::-1]]
		# Seed selected list with strongest cluster
		selected = [sorted_large[0]]
		# Add diverse clusters by minimum centroid distance
		for cluster_idx in sorted_large[1:]:
			# Stop once three clusters are selected
			if len(selected) >= 3: break
			# Keep clusters far enough from selected centroids
			if np.min(np.linalg.norm(kmeans.centroids[cluster_idx] - kmeans.centroids[selected], axis=1)) >= FAISS_DIVERSITY_DISTANCE:
				# Accept cluster index
				selected.append(cluster_idx)
		# Backfill remaining slots with next-largest clusters
		if len(selected) < 3:
			# Iterate sorted candidates in deterministic order
			for cluster_idx in sorted_large:
				# Stop once output size target is met
				if len(selected) >= 3: break
				# Add cluster when not already selected
				if cluster_idx not in selected: selected.append(cluster_idx)
		# Freeze selected cluster ids as numpy array
		top_clusters = np.array(selected)
	# Pick centroid vectors selected above
	top_centroids = kmeans.centroids[top_clusters]
	# Project selected centroids back to nearest real points
	_, idx = index.search(top_centroids, 1)
	# Convert nearest points to list output
	nearest = [points[i[0]].tolist() for i in idx]
	# Return deduped projected points
	return dedupe_points(nearest)

# Cluster one variant over all sampled taxa arrays and persist results

def run_variant(name: str, df: pl.DataFrame, paths: dict):
	# Map variant names to output path keys
	path_key = 'centroids_baseline' if name == 'baseline' else 'centroids_linear' if name == 'linear' else 'centroids_sqrt'
	# Resolve output path for this variant
	out_path = paths[path_key]
	# Track peak memory during clustering
	rss = RssSampler()
	# Start memory sampler
	rss.start()
	# Track wall time for variant
	t0 = time.time()
	# Hold output records for this variant
	records = []
	# Track fallback exactness counters
	low_input_total = 0
	# Track fallback exactness hits
	low_input_exact = 0
	# Iterate each taxon row for clustering
	for i, row in enumerate(df.iter_rows(named=True)):
		# Read input arrays from current row
		x_coords = row['x']
		# Read y arrays from current row
		y_coords = row['y']
		# Read optional weight array
		w_coords = row['w'] if 'w' in row else None
		# Build variant-specific weight vector
		weights = None if name == 'baseline' else w_coords if name == 'linear' else [float(np.sqrt(v)) for v in w_coords]
		# Run clustering for current taxon
		centroids = cluster_points(x_coords, y_coords, weights)
		# Evaluate low-input exact fallback behavior
		if len(x_coords) <= 3:
			# Increment low-input case count
			low_input_total += 1
			# Build rounded input points for exact fallback check
			in_points = dedupe_points([[x_coords[j], y_coords[j]] for j in range(len(x_coords))])
			# Count exact matches for fallback policy
			if in_points == centroids: low_input_exact += 1
		# Append result row
		records.append({'gbif_id': int(row['gbif_id']), 'centroids': centroids, 'n_input': len(x_coords)})
		# Emit progress every 2k taxa
		if (i + 1) % 2000 == 0: print(f"ABTEST : {name} clustered {i+1:,}/{len(df):,}")
	# Stop memory sampler
	rss_meta = rss.stop()
	# Build output dataframe
	out_df = pl.DataFrame(records)
	# Persist output dataframe
	out_df.write_parquet(out_path)
	# Build run metadata summary
	meta = {
		'variant': name,
		'rows': len(out_df),
		'time_s': time.time() - t0,
		'taxa_per_s': len(out_df) / max(0.001, (time.time() - t0)),
		'rss': rss_meta,
		'low_input_total': low_input_total,
		'low_input_exact': low_input_exact,
		'low_input_exact_rate': (low_input_exact / low_input_total) if low_input_total else 1.0,
		'output_path': out_path,
	}
	# Log variant summary line
	print(f"ABTEST : {name} done in {meta['time_s']:.1f}s ({meta['taxa_per_s']:.1f} taxa/s), peak+{rss_meta['peak_delta_gb']:.2f}GB")
	# Return output dataframe and metadata
	return out_df, meta

# Compute haversine distance in kilometers between two [lng, lat] points

def haversine_km(a: list, b: list) -> float:
	# Convert input degrees to radians
	lon1, lat1 = np.radians([a[0], a[1]])
	# Convert target degrees to radians
	lon2, lat2 = np.radians([b[0], b[1]])
	# Compute longitude delta
	dlon = lon2 - lon1
	# Compute latitude delta
	dlat = lat2 - lat1
	# Compute haversine core term
	h = np.sin(dlat / 2.0) ** 2 + np.cos(lat1) * np.cos(lat2) * np.sin(dlon / 2.0) ** 2
	# Convert angular distance to kilometers on Earth mean radius
	return float(6371.0088 * (2.0 * np.arcsin(np.sqrt(h))))

# Compare candidate centroids against baseline centroids and return percentile metrics

def compare_against_baseline(baseline: pl.DataFrame, candidate: pl.DataFrame) -> dict:
	# Join baseline and candidate by gbif id
	joined = baseline.join(candidate, on='gbif_id', how='inner', suffix='_cand')
	# Hold nearest-distance values for percentile metrics
	deltas = []
	# Iterate taxon pairs to compute nearest-centroid deltas
	for row in joined.iter_rows(named=True):
		# Read baseline centroid list
		base_pts = row['centroids']
		# Read candidate centroid list
		cand_pts = row['centroids_cand']
		# Skip rows lacking either side
		if not base_pts or not cand_pts: continue
		# Compute nearest baseline distance for each candidate centroid
		for c in cand_pts:
			# Compute distances to all baseline centroids
			d = [haversine_km(c, b) for b in base_pts]
			# Append nearest-centroid distance
			deltas.append(min(d))
	# Return empty metrics when no deltas available
	if len(deltas) == 0:
		return {'pairs': 0, 'p50_km': None, 'p95_km': None, 'p99_km': None, 'mean_km': None, 'max_km': None}
	# Convert deltas to numpy for percentile math
	arr = np.array(deltas, dtype=np.float64)
	# Return percentile and summary metrics
	return {
		'pairs': int(len(arr)),
		'p50_km': float(np.percentile(arr, 50)),
		'p95_km': float(np.percentile(arr, 95)),
		'p99_km': float(np.percentile(arr, 99)),
		'mean_km': float(arr.mean()),
		'max_km': float(arr.max()),
	}

# Main entrypoint for centroid A B C benchmark

def main():
	# Parse script arguments
	parser = argparse.ArgumentParser()
	# Accept release id for deterministic cache naming and joins
	parser.add_argument('--release-id', required=True)
	# Accept sample size for screening run
	parser.add_argument('--sample-size', type=int, default=50000)
	# Accept deterministic seed for reproducible sample selection
	parser.add_argument('--seed', type=int, default=42)
	# Allow forcing habitat cache rebuild
	parser.add_argument('--rebuild-cache', action='store_true')
	# Allow forcing sample + input cache rebuild
	parser.add_argument('--rebuild-inputs', action='store_true')
	# Select variants to run in this invocation
	parser.add_argument('--variants', default='baseline,linear,sqrt')
	# Parse args
	args = parser.parse_args()
	# Ensure temp directory exists for cache outputs
	os.makedirs(TMP_DIR, exist_ok=True)
	# Resolve release parquet to fail fast on typos
	release_parquet = os.path.join(RELEASES_DIR, args.release_id, f"{args.release_id}.parquet")
	# Abort when requested release parquet is missing
	if not os.path.exists(release_parquet): raise FileNotFoundError(f"Release parquet not found: {release_parquet}")
	# Build all benchmark file paths
	paths = build_paths(args.release_id)
	# Ensure full habitat cache is available
	ensure_habitat_cache(args.release_id, paths, args.rebuild_cache)
	# Build or load deterministic stratified sample
	sample = build_stratified_sample(paths, args.sample_size, args.seed, args.rebuild_inputs)
	# Build baseline raw arrays cache for sampled taxa
	raw_arrays = build_raw_arrays_cache(paths, sample, args.rebuild_inputs)
	# Build habitat arrays cache for sampled taxa
	habitat_arrays = build_habitat_arrays_cache(paths, sample, args.rebuild_inputs)
	# Keep only taxa present in both baseline and candidate inputs
	common_ids = set(raw_arrays['gbif_id'].to_list()).intersection(set(habitat_arrays['gbif_id'].to_list()))
	# Filter baseline arrays to common taxon ids
	raw_arrays = raw_arrays.filter(pl.col('gbif_id').is_in(common_ids)).sort('gbif_id')
	# Filter habitat arrays to common taxon ids
	habitat_arrays = habitat_arrays.filter(pl.col('gbif_id').is_in(common_ids)).sort('gbif_id')
	# Log final comparable taxon count
	print(f"ABTEST : Comparable taxa in sample {len(raw_arrays):,}")
	# Parse selected variants list
	variants = [v.strip() for v in args.variants.split(',') if v.strip()]
	# Hold outputs by variant name
	outputs = {}
	# Hold metadata by variant name
	meta = {}
	# Run baseline variant when selected
	if 'baseline' in variants:
		# Cluster baseline from raw arrays
		outputs['baseline'], meta['baseline'] = run_variant('baseline', raw_arrays, paths)
	# Run linear-weighted habitat variant when selected
	if 'linear' in variants:
		# Cluster linear variant from habitat arrays
		outputs['linear'], meta['linear'] = run_variant('linear', habitat_arrays, paths)
	# Run sqrt-weighted habitat variant when selected
	if 'sqrt' in variants:
		# Cluster sqrt variant from habitat arrays
		outputs['sqrt'], meta['sqrt'] = run_variant('sqrt', habitat_arrays, paths)
	# Load cached baseline output when baseline was not run in this invocation
	if 'baseline' not in outputs and os.path.exists(paths['centroids_baseline']):
		# Reuse baseline centroids from prior run
		outputs['baseline'] = pl.read_parquet(paths['centroids_baseline'])
	# Build comparison metrics against baseline when available
	comparison = {
		'release_id': args.release_id,
		'sample_size_requested': args.sample_size,
		'sample_size_comparable': int(len(raw_arrays)),
		'meta': meta,
		'quality': {},
	}
	# Compare linear variant to baseline
	if 'baseline' in outputs and 'linear' in outputs:
		comparison['quality']['linear_vs_baseline'] = compare_against_baseline(outputs['baseline'], outputs['linear'])
	# Compare sqrt variant to baseline
	if 'baseline' in outputs and 'sqrt' in outputs:
		comparison['quality']['sqrt_vs_baseline'] = compare_against_baseline(outputs['baseline'], outputs['sqrt'])
	# Write comparison json for downstream review
	with open(paths['comparison'], 'w', encoding='utf-8') as f:
		# Persist human-readable benchmark summary
		json.dump(comparison, f, indent=2)
	# Log comparison artifact path
	print(f"ABTEST : Wrote comparison summary {paths['comparison']}")
	# Print compact json summary to console
	print(json.dumps(comparison, indent=2))

# Run main when executed as a script
if __name__ == '__main__':
	# Execute benchmark flow
	main()
