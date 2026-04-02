# 08 rollup and habitat benchmark
# Purpose: compare rollup and habitat construction strategies against current geo pipeline logic.
# Outcome summary:
# - This script is exploratory and profiling-focused, not a production replacement script.
# - Keep production behavior aligned with pipeline/geo.py unless this benchmark shows clear full-run wins.
# - Packaging-speed conclusions are tracked in 09_packaging_profile.py and summarized in 06_geo_rework_findings.md.
# Dead ends to avoid:
# - Do not use broad geo JSON re-expansion (`json_each`) for large reconstruction workflows in this script family.
# - Prefer native stage outputs and deterministic partitioned profiling for heavy tests.
# Reference:
# - Read 06_geo_rework_findings.md for consolidated decisions and historical context.

# Load argument parsing for benchmark mode selection
import argparse
# Load json for machine-readable benchmark output
import json
# Load filesystem helpers for temp artifact paths
import os
# Load subprocess-safe timing helper
import time
# Load thread helpers for memory sampler loop
import threading
# Load duckdb for release-table parentage lookup
import duckdb
# Load polars for columnar rollup and habitat aggregation
import polars as pl

# Try to load psutil for rss peak sampling
try:
	# Import psutil for process memory metrics
	import psutil
# Handle environments where psutil is missing
except Exception:
	# Keep psutil unset when unavailable
	psutil = None

# Keep Mercator latitude limit aligned with geo pipeline
MAX_LAT = 85.0511
# Keep zoom level aligned with geo pipeline
TILE_LEVEL = 10
# Keep tile count aligned with geo pipeline
TILE_COUNT = 2 ** TILE_LEVEL
# Keep family-level cutoff ranks aligned with current geo plan
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

# Load compact occurrences exactly like pipeline load stage
def load_occurrences(occurrence_parquet: str, release_parquet: str, taxa_limit: int | None) -> pl.DataFrame:
	# Build accepted taxa lazy frame for streaming join
	accepted_lf = (
		pl.scan_parquet(release_parquet)
		.filter(pl.col("accepted"), pl.col("gbif_id").is_not_null())
		.select(pl.col("gbif_id").alias("taxon"))
		.unique()
	)
	# Apply optional accepted-taxa cap for faster micro-bench loops
	if taxa_limit is not None:
		# Materialize and cap accepted ids deterministically
		accepted_ids = accepted_lf.collect().head(taxa_limit)
		# Rebuild accepted lazy frame from capped ids
		accepted_lf = accepted_ids.lazy()
	# Build compact taxon arrays from occurrences
	compact = (
		pl.scan_parquet(occurrence_parquet)
		.select("taxon", "location", "elevation", "spatial_issue")
		.filter(~pl.col("spatial_issue"))
		.join(accepted_lf, on="taxon", how="inner")
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
		.collect(engine="streaming")
	)
	# Return compact occurrence arrays
	return compact

# Load parentage with parent rank from release parquet
def load_parentage(release_parquet: str) -> pl.DataFrame:
	# Load accepted taxa with id and parent pointers
	taxa = (
		pl.scan_parquet(release_parquet)
		.filter(pl.col("accepted"), pl.col("gbif_id").is_not_null())
		.select("gbif_id", "id_meso", "parent_consensus", "rank_consensus")
		.collect()
	)
	# Build child->parent edges with parent rank
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
		.filter(pl.col("gbif_id") != pl.col("parent_gbif"))
	)
	# Return parent edge table
	return parentage

# Roll up raw occurrences recursively to family-level parents
def rollup_to_family(compact: pl.DataFrame, parentage: pl.DataFrame) -> pl.DataFrame:
	# Keep only family-and-below parent edges
	low_parentage = parentage.filter(pl.col("parent_rank").is_in(RAW_ROLLUP_PARENT_RANKS)).select("gbif_id", "parent_gbif")
	# Seed frontier with all direct rows
	frontier = compact
	# Keep all level contributions for one final merge
	parts = [compact]
	# Iterate upward until no more qualifying parents
	while True:
		# Push current frontier one step upward and concat child arrays
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
		# Stop when frontier is empty
		if frontier.is_empty(): break
		# Keep this level for final merge
		parts.append(frontier)
	# Merge direct + rolled contributions by taxon
	rolled = pl.concat(parts, how="vertical").group_by("taxon").agg(
		pl.col("lng").list.explode(),
		pl.col("lat").list.explode(),
		pl.col("elevation").list.explode(),
	)
	# Return family-level raw rolled arrays
	return rolled

# Build habitat tiles in one pass (current pipeline style)
def build_habitat_tiles_single(compact: pl.DataFrame) -> pl.DataFrame:
	# Compute tile counts per taxon using list-eval and value_counts
	habitat_tiles = (
		compact
		.with_columns(
			pl.col("lng").list.eval(
				((pl.element().cast(pl.Float64) + 180.0) / 360.0 * TILE_COUNT).clip(0, TILE_COUNT - 1).cast(pl.UInt32)
			).alias("tx"),
			pl.col("lat").list.eval(
				((1.0 - ((pl.element().cast(pl.Float64).clip(-MAX_LAT, MAX_LAT) * (3.141592653589793 / 180.0)).tan() +
					(1.0 / (pl.element().cast(pl.Float64).clip(-MAX_LAT, MAX_LAT) * (3.141592653589793 / 180.0)).cos())).log()
					/ 3.141592653589793) / 2.0 * TILE_COUNT).clip(0, TILE_COUNT - 1).cast(pl.UInt32)
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
	# Return one-pass habitat tiles
	return habitat_tiles

# Build habitat tiles in taxon batches then merge counts
def build_habitat_tiles_chunked(compact: pl.DataFrame, batch_taxa: int) -> pl.DataFrame:
	# Hold per-batch habitat outputs
	chunks = []
	# Iterate compact rows by taxon batch size
	for start in range(0, len(compact), batch_taxa):
		# Slice current taxon batch
		batch = compact.slice(start, batch_taxa)
		# Build habitat rows for current batch using the same expression path
		chunk = build_habitat_tiles_single(batch)
		# Keep current chunk for final merge
		chunks.append(chunk)
	# Merge all chunk outputs and sum duplicate keys
	merged = pl.concat(chunks, how="vertical").group_by("gbif_id", "tile_id").agg(pl.col("count").sum().alias("count"))
	# Return merged chunked habitat output
	return merged

# Build elevation profile bins and exact median ints while raw arrays still exist
# Returns per-taxon medians plus per-taxon per-bin counts
# Bin granularity uses 100m steps
# Median is exact here because we still have full elevation lists
# This stage is intentionally before upper-rank rollups
# so we can discard raw occurrence arrays afterward
def build_low_elevation_stats(compact: pl.DataFrame) -> tuple[pl.DataFrame, pl.DataFrame]:
	# Compute exact median integer from per-taxon elevation list
	medians = compact.select(
		pl.col("taxon").alias("gbif_id"),
		pl.col("elevation").list.median().round(0).cast(pl.Int32).alias("median_elev")
	)
	# Compute elevation bins and counts inside each taxon's list
	bins = (
		compact
		.with_columns(
			pl.col("elevation")
			.list.eval(((pl.element().cast(pl.Int32) // 100) * 100))
			.list.eval(pl.element().value_counts(sort=True))
			.alias("elev_counts")
		)
		.select(pl.col("taxon").alias("gbif_id"), "elev_counts")
		.explode("elev_counts")
		.unnest("elev_counts")
		.rename({"": "elev_bin", "count": "bin_count"})
		.select("gbif_id", pl.col("elev_bin").cast(pl.Int32), pl.col("bin_count").cast(pl.Int64))
	)
	# Return exact low medians and low bins
	return medians, bins

# Roll up habitat rows from above family ranks by additive count sums
# This keeps rollups mergeable and avoids carrying raw arrays to top ranks
def rollup_habitat_upper(habitat_low: pl.DataFrame, parentage: pl.DataFrame) -> pl.DataFrame:
	# Keep only above-family parent edges
	high_parentage = parentage.filter(~pl.col("parent_rank").is_in(RAW_ROLLUP_PARENT_RANKS)).select("gbif_id", "parent_gbif")
	# Seed frontier with low habitat rows
	frontier = habitat_low.select("gbif_id", "tile_id", "count")
	# Keep direct and rolled parts for final additive merge
	parts = [frontier]
	# Iterate parent propagation until frontier is empty
	while True:
		# Roll one step upward and aggregate per parent-tile key
		frontier = (
			frontier
			.join(high_parentage, on="gbif_id", how="inner")
			.group_by("parent_gbif", "tile_id")
			.agg(pl.col("count").sum().alias("count"))
			.rename({"parent_gbif": "gbif_id"})
			.with_columns(pl.col("gbif_id").cast(habitat_low.schema["gbif_id"]))
		)
		# Stop when no higher-rank rows remain
		if frontier.is_empty(): break
		# Keep current frontier for final additive merge
		parts.append(frontier)
	# Merge all levels and sum duplicate keys
	rolled = pl.concat(parts, how="vertical").group_by("gbif_id", "tile_id").agg(pl.col("count").sum().alias("count"))
	# Return full habitat table including higher ranks
	return rolled

# Roll up elevation profile bins above family using additive bin counts
# This allows deriving medians at higher ranks without raw arrays
def rollup_elevation_bins_upper(low_bins: pl.DataFrame, parentage: pl.DataFrame) -> pl.DataFrame:
	# Keep only above-family parent edges
	high_parentage = parentage.filter(~pl.col("parent_rank").is_in(RAW_ROLLUP_PARENT_RANKS)).select("gbif_id", "parent_gbif")
	# Seed frontier with low elevation bins
	frontier = low_bins.select("gbif_id", "elev_bin", "bin_count")
	# Keep direct and rolled parts for final additive merge
	parts = [frontier]
	# Iterate parent propagation until frontier is empty
	while True:
		# Roll one step upward and aggregate per parent-bin key
		frontier = (
			frontier
			.join(high_parentage, on="gbif_id", how="inner")
			.group_by("parent_gbif", "elev_bin")
			.agg(pl.col("bin_count").sum().alias("bin_count"))
			.rename({"parent_gbif": "gbif_id"})
			.with_columns(pl.col("gbif_id").cast(low_bins.schema["gbif_id"]))
		)
		# Stop when no higher-rank rows remain
		if frontier.is_empty(): break
		# Keep current frontier for final additive merge
		parts.append(frontier)
	# Merge all levels and sum duplicate keys
	rolled = pl.concat(parts, how="vertical").group_by("gbif_id", "elev_bin").agg(pl.col("bin_count").sum().alias("bin_count"))
	# Return full elevation-bin table including higher ranks
	return rolled

# Derive integer medians from elevation bin histograms
# Uses lower bound of first bin crossing half-count threshold
# as a simple deterministic integer median surrogate
# for above-family ranks where exact arrays are no longer kept
def medians_from_bins(all_bins: pl.DataFrame) -> pl.DataFrame:
	# Open in-memory duckdb for windowed median derivation
	with duckdb.connect(":memory:") as db:
		# Register bin table from polars arrow
		db.register("bins", all_bins.to_arrow())
		# Compute first bin reaching half cumulative count per taxon
		result = db.execute("""
			WITH ordered AS (
				SELECT
					gbif_id,
					elev_bin,
					bin_count,
					sum(bin_count) OVER (PARTITION BY gbif_id ORDER BY elev_bin ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS cum_count,
					sum(bin_count) OVER (PARTITION BY gbif_id) AS total_count
				FROM bins
			),
			picked AS (
				SELECT
					gbif_id,
					min(elev_bin) FILTER (WHERE cum_count >= (total_count + 1) / 2.0) AS median_elev
				FROM ordered
				GROUP BY gbif_id
			)
			SELECT gbif_id, cast(median_elev AS INTEGER) AS median_elev
			FROM picked
		""").pl()
	# Return derived median table
	return result

# Measure peak rss while running a callable
def run_with_peak_memory(fn):
	# Keep timing baseline
	t0 = time.perf_counter()
	# Keep peak rss tracker in bytes
	peak = 0
	# Keep stop signal for sampler thread
	stop = False
	# Track current process handle when psutil is available
	proc = psutil.Process(os.getpid()) if psutil else None
	# Sample process rss in a lightweight loop
	def sampler():
		# Capture outer variables in thread scope
		nonlocal peak, stop
		# Poll rss until stop signal arrives
		while not stop:
			# Update peak rss when psutil is available
			if proc:
				# Keep max rss seen so far
				peak = max(peak, int(proc.memory_info().rss))
			# Sleep briefly between polls
			time.sleep(0.05)
	# Start memory sampler thread
	thread = threading.Thread(target=sampler, daemon=True)
	# Launch sampler
	thread.start()
	# Run target callable and keep return value
	result = fn()
	# Stop sampler loop
	stop = True
	# Wait briefly for sampler shutdown
	thread.join(timeout=1.0)
	# Compute elapsed seconds
	elapsed = time.perf_counter() - t0
	# Return callable output and metrics
	return result, elapsed, peak

# Run selected benchmark workflow
def main():
	# Build CLI parser for prep/variant phases
	parser = argparse.ArgumentParser()
	# Select benchmark phase
	parser.add_argument("--mode", choices=["prepare", "single", "chunked", "elev_single", "elev_chunked"], required=True)
	# Select release version to test
	parser.add_argument("--release", default="20260401-7e70f9623fe7")
	# Optional accepted-taxa cap for quick dry runs
	parser.add_argument("--taxa-limit", type=int, default=None)
	# Select chunk batch size for chunked variant
	parser.add_argument("--batch-taxa", type=int, default=20000)
	# Parse args
	args = parser.parse_args()
	# Resolve canonical benchmark paths
	base = os.path.join("data", "releases", args.release)
	# Resolve release parquet path
	release_parquet = os.path.join(base, f"{args.release}.parquet")
	# Resolve occurrences parquet path
	occurrence_parquet = os.path.join("data", "geo", "occurrences.parquet")
	# Resolve temp path for prepared low-rollup table
	low_rollup_path = os.path.join("data", "temp", f"abtest_low_rollup_{args.release}.parquet")
	# Ensure temp directory exists
	os.makedirs(os.path.dirname(low_rollup_path), exist_ok=True)

	# Run prepare phase to materialize low-rollup arrays once
	if args.mode == "prepare":
		# Build compact occurrences with optional cap
		compact, t_compact, m_compact = run_with_peak_memory(lambda: load_occurrences(occurrence_parquet, release_parquet, args.taxa_limit))
		# Load parentage edge table
		parentage, t_parent, m_parent = run_with_peak_memory(lambda: load_parentage(release_parquet))
		# Build family-level raw rollup
		rolled, t_roll, m_roll = run_with_peak_memory(lambda: rollup_to_family(compact, parentage))
		# Persist low-rollup artifact for variant runs
		rolled.write_parquet(low_rollup_path)
		# Emit prep metrics
		print(json.dumps({
			"mode": "prepare",
			"low_rollup_path": low_rollup_path,
			"compact_taxa": len(compact),
			"rolled_taxa": len(rolled),
			"t_compact_s": round(t_compact, 3),
			"t_parentage_s": round(t_parent, 3),
			"t_rollup_s": round(t_roll, 3),
			"peak_compact_mb": round(m_compact / (1024 * 1024), 1),
			"peak_parentage_mb": round(m_parent / (1024 * 1024), 1),
			"peak_rollup_mb": round(m_roll / (1024 * 1024), 1),
		}, indent=2))
		# Stop after prepare output
		return

	# Load prepared low-rollup artifact for variant runs
	rolled = pl.read_parquet(low_rollup_path)
	# Load parentage table for upper-rank rollups
	parentage = load_parentage(release_parquet)

	# Run one-pass habitat variant
	if args.mode == "single":
		# Execute one-pass habitat build with metrics
		habitat, elapsed, peak = run_with_peak_memory(lambda: build_habitat_tiles_single(rolled))
		# Emit one-pass metrics
		print(json.dumps({
			"mode": "single",
			"rolled_taxa": len(rolled),
			"habitat_rows": len(habitat),
			"habitat_taxa": habitat["gbif_id"].n_unique(),
			"elapsed_s": round(elapsed, 3),
			"peak_mb": round(peak / (1024 * 1024), 1),
			"sum_count": int(habitat["count"].sum()),
		}, indent=2))
		# Stop after one-pass output
		return

	# Run chunked habitat variant
	if args.mode == "chunked":
		# Execute chunked habitat build with metrics
		habitat, elapsed, peak = run_with_peak_memory(lambda: build_habitat_tiles_chunked(rolled, args.batch_taxa))
		# Emit chunked metrics
		print(json.dumps({
			"mode": "chunked",
			"batch_taxa": args.batch_taxa,
			"rolled_taxa": len(rolled),
			"habitat_rows": len(habitat),
			"habitat_taxa": habitat["gbif_id"].n_unique(),
			"elapsed_s": round(elapsed, 3),
			"peak_mb": round(peak / (1024 * 1024), 1),
			"sum_count": int(habitat["count"].sum()),
		}, indent=2))
		# Stop after chunked output
		return

	# Choose habitat builder for elevation-integrity runs
	hab_builder = (lambda: build_habitat_tiles_single(rolled)) if args.mode == "elev_single" else (lambda: build_habitat_tiles_chunked(rolled, args.batch_taxa))
	# Build low habitat with selected builder
	hab_low, t_hab, m_hab = run_with_peak_memory(hab_builder)
	# Roll habitat to higher ranks
	hab_all, t_hab_roll, m_hab_roll = run_with_peak_memory(lambda: rollup_habitat_upper(hab_low, parentage))
	# Build low exact medians and low elevation bins
	low_medians, low_bins = build_low_elevation_stats(rolled)
	# Roll elevation bins to higher ranks
	all_bins, t_bin_roll, m_bin_roll = run_with_peak_memory(lambda: rollup_elevation_bins_upper(low_bins, parentage))
	# Derive high-rank medians from rolled bins
	high_medians = medians_from_bins(all_bins)
	# Merge low exact medians over derived medians for low taxa precision
	all_medians = (
		high_medians
		.join(low_medians, on="gbif_id", how="full", suffix="_low")
		.select(
			pl.coalesce([pl.col("gbif_id"), pl.col("gbif_id_low")]).alias("gbif_id"),
			pl.coalesce([pl.col("median_elev_low"), pl.col("median_elev")]).cast(pl.Int32).alias("median_elev"),
		)
	)
	# Compute integrity counts for habitat/profile/median coverage
	hab_taxa = hab_all.select("gbif_id").unique()
	bin_taxa = all_bins.select("gbif_id").unique()
	med_taxa = all_medians.select("gbif_id").unique()
	missing_profile = hab_taxa.join(bin_taxa, on="gbif_id", how="anti").height
	missing_median = hab_taxa.join(med_taxa, on="gbif_id", how="anti").height
	# Emit elevation-integrity benchmark output
	print(json.dumps({
		"mode": args.mode,
		"batch_taxa": args.batch_taxa if args.mode == "elev_chunked" else None,
		"rolled_taxa": len(rolled),
		"hab_low_rows": len(hab_low),
		"hab_all_rows": len(hab_all),
		"hab_all_taxa": hab_all["gbif_id"].n_unique(),
		"elev_bin_rows": len(all_bins),
		"elev_bin_taxa": all_bins["gbif_id"].n_unique(),
		"median_taxa": all_medians["gbif_id"].n_unique(),
		"missing_profile_taxa": int(missing_profile),
		"missing_median_taxa": int(missing_median),
		"hab_sum_count": int(hab_all["count"].sum()),
		"elev_sum_count": int(all_bins["bin_count"].sum()),
		"t_hab_s": round(t_hab, 3),
		"t_hab_roll_s": round(t_hab_roll, 3),
		"t_bin_roll_s": round(t_bin_roll, 3),
		"peak_hab_mb": round(m_hab / (1024 * 1024), 1),
		"peak_hab_roll_mb": round(m_hab_roll / (1024 * 1024), 1),
		"peak_bin_roll_mb": round(m_bin_roll / (1024 * 1024), 1),
	}, indent=2))
# Run CLI entrypoint
if __name__ == "__main__":
	# Start selected benchmark mode
	main()
