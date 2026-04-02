# 09 packaging profile benchmark
# Purpose: profile packaging variants against the same staged inputs with durable step timings.
# Outcome summary from current measured runs:
# - Variant A (current create+alter+updates shape) was fastest on full data.
# - Variant C (pre-agg + single CTAS join) was significantly slower.
# - Variant D (staged-table join flavor) was better than C but still slower than A.
# What this means:
# - The dominant cost is habitat aggregation over ~70M rows, not centroid/profile merge steps.
# - Keep variant-A semantics for speed; improve reliability via stage spooling/partitioning.
# Dead ends to avoid:
# - Do not interpret raw EXCEPT-based symmetric diffs on JSON payloads as semantic mismatch without normalization.
# - Do not use broad geo JSON reconstruction (`json_each`) as a prep path for large runs.
# Reference:
# - Read 06_geo_rework_findings.md for consolidated decisions and rationale.

# Load cli argument parsing
import argparse
# Load filesystem helpers
import os
# Load timestamp helpers for log naming
from datetime import datetime
# Load timer for step timings
import time
# Load traceback formatting for failure logs
import traceback
# Load json for structured summary output
import json
# Load duckdb for SQL benchmark runs
import duckdb

# Write one timestamped line to stdout and log file
def log_line(log_path: str, message: str):
	# Build timestamp prefix for easier timeline debugging
	timestamp = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
	# Build final log line
	line = f"[{timestamp}] {message}"
	# Print line immediately for terminal progress
	print(line, flush=True)
	# Append line to persistent log file for partial-result recovery
	with open(log_path, 'a', encoding='utf-8') as file:
		# Persist line with trailing newline
		file.write(line + '\n')

# Execute one SQL statement with timing and durable step logging
def timed_execute(db: duckdb.DuckDBPyConnection, log_path: str, label: str, sql: str, params: list | None = None) -> float:
	# Announce step start
	log_line(log_path, f"START {label}")
	# Capture start timestamp
	t0 = time.perf_counter()
	# Run SQL statement
	db.execute(sql, params or [])
	# Compute elapsed seconds
	elapsed = time.perf_counter() - t0
	# Announce step completion
	log_line(log_path, f"DONE  {label} {elapsed:.3f}s")
	# Return elapsed duration
	return elapsed

# Build partition predicate used for safe small-slice benchmarking
def partition_predicate(alias: str, modulo: int, remainder: int) -> str:
	# Return always-true predicate when modulo is one
	if modulo <= 1: return 'TRUE'
	# Return deterministic hash partition predicate for gbif_id
	return f"abs(hash({alias}.gbif_id)) % {modulo} = {remainder}"

# Run baseline variant A from current geo packaging shape
def run_variant_a(db: duckdb.DuckDBPyConnection, log_path: str) -> dict:
	# Hold per-step timings for this variant
	steps = {}
	# Create habitat-backed base table with ordered habitat json
	steps['create_geo'] = timed_execute(db, log_path, 'A create geo', """
		CREATE TABLE geo_a AS
		SELECT
			gbif_id,
			array_agg(
				struct_pack(lng := center_lng, lat := center_lat, occ := count)
				ORDER BY CASE WHEN count = 1 THEN 1 ELSE 0 END, count DESC
			)::JSON AS habitat,
			max(count) AS max,
			avg(count) AS avg
		FROM habitat_df
		GROUP BY gbif_id;
	""")
	# Add mutable payload columns
	steps['alter_geo'] = timed_execute(db, log_path, 'A alter geo', """
		ALTER TABLE geo_a ADD COLUMN IF NOT EXISTS centroids JSON;
		ALTER TABLE geo_a ADD COLUMN IF NOT EXISTS elevation SMALLINT;
		ALTER TABLE geo_a ADD COLUMN IF NOT EXISTS elevation_profile JSON;
	""")
	# Build cleaned centroid payload table
	steps['centroid_clean'] = timed_execute(db, log_path, 'A centroid clean', """
		CREATE TEMP TABLE centroid_clean_a AS
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
	# Merge cleaned centroids
	steps['update_centroids'] = timed_execute(db, log_path, 'A update centroids', """
		UPDATE geo_a SET centroids = centroid_clean_a.centroids
		FROM centroid_clean_a
		WHERE geo_a.gbif_id = centroid_clean_a.gbif_id;
	""")
	# Build elevation profile payload table
	steps['profile_clean'] = timed_execute(db, log_path, 'A profile clean', """
		CREATE TEMP TABLE elevation_profile_clean_a AS
		SELECT
			gbif_id,
			array_agg(struct_pack(elevation := elevation_bin, occ := bin_count) ORDER BY elevation_bin)::JSON AS elevation_profile
		FROM elevation_bins_df
		GROUP BY gbif_id;
	""")
	# Merge elevation profile payload
	steps['update_profile'] = timed_execute(db, log_path, 'A update profile', """
		UPDATE geo_a SET elevation_profile = elevation_profile_clean_a.elevation_profile
		FROM elevation_profile_clean_a
		WHERE geo_a.gbif_id = elevation_profile_clean_a.gbif_id;
	""")
	# Merge median elevations
	steps['update_medians'] = timed_execute(db, log_path, 'A update medians', """
		UPDATE geo_a SET elevation = CAST(elevation_medians_df.elevation AS SMALLINT)
		FROM elevation_medians_df
		WHERE geo_a.gbif_id = elevation_medians_df.gbif_id;
	""")
	# Compute total runtime for variant
	total = sum(steps.values())
	# Return timing map
	return {'total_seconds': total, 'steps': steps}

# Run alternative 1: pre-aggregate all payloads then single CTAS join
def run_variant_c(db: duckdb.DuckDBPyConnection, log_path: str) -> dict:
	# Hold per-step timings for this variant
	steps = {}
	# Build habitat aggregate once
	steps['habitat_agg'] = timed_execute(db, log_path, 'C habitat agg', """
		CREATE TEMP TABLE habitat_agg_c AS
		SELECT
			gbif_id,
			array_agg(
				struct_pack(lng := center_lng, lat := center_lat, occ := count)
				ORDER BY CASE WHEN count = 1 THEN 1 ELSE 0 END, count DESC
			)::JSON AS habitat,
			max(count) AS max,
			avg(count) AS avg
		FROM habitat_df
		GROUP BY gbif_id;
	""")
	# Build cleaned centroid payload once
	steps['centroid_clean'] = timed_execute(db, log_path, 'C centroid clean', """
		CREATE TEMP TABLE centroid_clean_c AS
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
	# Build elevation profile payload once
	steps['profile_clean'] = timed_execute(db, log_path, 'C profile clean', """
		CREATE TEMP TABLE elevation_profile_clean_c AS
		SELECT
			gbif_id,
			array_agg(struct_pack(elevation := elevation_bin, occ := bin_count) ORDER BY elevation_bin)::JSON AS elevation_profile
		FROM elevation_bins_df
		GROUP BY gbif_id;
	""")
	# Build final table in one pass via joins
	steps['create_geo'] = timed_execute(db, log_path, 'C create geo', """
		CREATE TABLE geo_c AS
		SELECT
			h.gbif_id,
			h.habitat,
			h.max,
			h.avg,
			c.centroids,
			CAST(m.elevation AS SMALLINT) AS elevation,
			ep.elevation_profile
		FROM habitat_agg_c h
		LEFT JOIN centroid_clean_c c ON h.gbif_id = c.gbif_id
		LEFT JOIN elevation_medians_df m ON h.gbif_id = m.gbif_id
		LEFT JOIN elevation_profile_clean_c ep ON h.gbif_id = ep.gbif_id;
	""")
	# Compute total runtime for variant
	total = sum(steps.values())
	# Return timing map
	return {'total_seconds': total, 'steps': steps}

# Run alternative 2: two-stage DuckDB build with persisted aggregates and final join
# This favors restartability and lower intermediate rework in large runs
# while keeping SQL in columnar engine
# Note: still computes same heavy habitat aggregate cost once
# but isolates build phases for operational stability
# and easier reuse across reruns
# (e.g. rebuild final table without recomputing habitat aggregate)
def run_variant_d(db: duckdb.DuckDBPyConnection, log_path: str) -> dict:
	# Hold per-step timings for this variant
	steps = {}
	# Build habitat aggregate stage table
	steps['habitat_stage'] = timed_execute(db, log_path, 'D habitat stage', """
		CREATE TABLE habitat_stage_d AS
		SELECT
			gbif_id,
			array_agg(
				struct_pack(lng := center_lng, lat := center_lat, occ := count)
				ORDER BY CASE WHEN count = 1 THEN 1 ELSE 0 END, count DESC
			)::JSON AS habitat,
			max(count) AS max,
			avg(count) AS avg
		FROM habitat_df
		GROUP BY gbif_id;
	""")
	# Build cleaned centroid stage table
	steps['centroid_stage'] = timed_execute(db, log_path, 'D centroid stage', """
		CREATE TABLE centroid_stage_d AS
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
	# Build elevation profile stage table
	steps['profile_stage'] = timed_execute(db, log_path, 'D profile stage', """
		CREATE TABLE profile_stage_d AS
		SELECT
			gbif_id,
			array_agg(struct_pack(elevation := elevation_bin, occ := bin_count) ORDER BY elevation_bin)::JSON AS elevation_profile
		FROM elevation_bins_df
		GROUP BY gbif_id;
	""")
	# Build final table from persisted stage tables
	steps['create_geo'] = timed_execute(db, log_path, 'D create geo', """
		CREATE TABLE geo_d AS
		SELECT
			h.gbif_id,
			h.habitat,
			h.max,
			h.avg,
			c.centroids,
			CAST(m.elevation AS SMALLINT) AS elevation,
			p.elevation_profile
		FROM habitat_stage_d h
		LEFT JOIN centroid_stage_d c ON h.gbif_id = c.gbif_id
		LEFT JOIN elevation_medians_df m ON h.gbif_id = m.gbif_id
		LEFT JOIN profile_stage_d p ON h.gbif_id = p.gbif_id;
	""")
	# Compute total runtime for variant
	total = sum(steps.values())
	# Return timing map
	return {'total_seconds': total, 'steps': steps}

# Build shared source temp tables with optional gbif partition filter
def build_inputs(db: duckdb.DuckDBPyConnection, log_path: str, habitat_path: str, centroids_path: str, elev_bins_path: str, elev_medians_path: str, modulo: int, remainder: int):
	# Build partition predicates for each source alias
	h_pred = partition_predicate('h', modulo, remainder)
	# Build centroids partition predicate
	c_pred = partition_predicate('c', modulo, remainder)
	# Build bins partition predicate
	b_pred = partition_predicate('b', modulo, remainder)
	# Build medians partition predicate
	m_pred = partition_predicate('m', modulo, remainder)
	# Load habitat source rows with partition predicate
	timed_execute(db, log_path, 'load habitat_df', f"""
		CREATE TEMP TABLE habitat_df AS
		SELECT h.*
		FROM read_parquet(?) h
		WHERE {h_pred};
	""", [habitat_path])
	# Load centroids source rows with partition predicate
	timed_execute(db, log_path, 'load centroid_df', f"""
		CREATE TEMP TABLE centroid_df AS
		SELECT c.*
		FROM read_parquet(?) c
		WHERE {c_pred};
	""", [centroids_path])
	# Load elevation bins source rows with partition predicate
	timed_execute(db, log_path, 'load elevation_bins_df', f"""
		CREATE TEMP TABLE elevation_bins_df AS
		SELECT b.*
		FROM read_parquet(?) b
		WHERE {b_pred};
	""", [elev_bins_path])
	# Load elevation medians source rows with partition predicate
	timed_execute(db, log_path, 'load elevation_medians_df', f"""
		CREATE TEMP TABLE elevation_medians_df AS
		SELECT m.*
		FROM read_parquet(?) m
		WHERE {m_pred};
	""", [elev_medians_path])

# Entrypoint
def main():
	# Build parser
	parser = argparse.ArgumentParser()
	# Choose release token used in dump file names
	parser.add_argument('--release', default='20260401-7e70f9623fe7')
	# Choose directory containing dump parquets
	parser.add_argument('--dump-dir', default='importer/canopy/data/temp')
	# Choose hash partition modulo for smaller profiling run
	parser.add_argument('--modulo', type=int, default=16)
	# Choose hash partition remainder for deterministic slice
	parser.add_argument('--remainder', type=int, default=0)
	# Choose duckdb thread count for run consistency
	parser.add_argument('--threads', type=int, default=8)
	# Choose optional explicit log file path
	parser.add_argument('--log-file', default='')
	# Parse cli args
	args = parser.parse_args()

	# Resolve dump input paths
	habitat_path = os.path.join(args.dump_dir, f'geo_abtest_habitat_{args.release}.parquet')
	# Resolve centroid dump path
	centroids_path = os.path.join(args.dump_dir, f'geo_abtest_centroids_{args.release}.parquet')
	# Resolve elevation-bin dump path
	elev_bins_path = os.path.join(args.dump_dir, f'geo_abtest_elevation_bins_{args.release}.parquet')
	# Resolve elevation-median dump path
	elev_medians_path = os.path.join(args.dump_dir, f'geo_abtest_elevation_medians_{args.release}.parquet')

	# Validate dump files exist before running benchmark
	for path in [habitat_path, centroids_path, elev_bins_path, elev_medians_path]:
		# Abort when any required input is missing
		if not os.path.isfile(path): raise FileNotFoundError(f'Missing dump input: {path}')

	# Resolve default log file path when none was provided
	log_path = args.log_file or os.path.join(args.dump_dir, f'packaging_profile_{args.release}_m{args.modulo}_r{args.remainder}.log')
	# Truncate existing log file to keep this run clean
	with open(log_path, 'w', encoding='utf-8') as file:
		# Seed log with run header
		file.write('')

	# Announce run configuration
	log_line(log_path, f'RUN release={args.release} dump_dir={args.dump_dir} modulo={args.modulo} remainder={args.remainder} threads={args.threads}')

	# Open in-memory duckdb for benchmark execution
	with duckdb.connect(':memory:') as db:
		# Route spill files to dump dir temp area
		db.execute(f"SET temp_directory = '{args.dump_dir}'")
		# Keep larger Arrow strings enabled for safety
		db.execute('SET arrow_large_buffer_size=true')
		# Apply requested thread count
		db.execute(f'SET threads={max(1, args.threads)}')
		# Disable insertion order preservation for better perf in bulk ops
		db.execute('SET preserve_insertion_order=false')
		# Build source temp tables with deterministic partitioning
		build_inputs(db, log_path, habitat_path, centroids_path, elev_bins_path, elev_medians_path, args.modulo, args.remainder)
		# Capture source cardinalities for context
		source_stats = {
			'habitat_rows': db.execute('SELECT COUNT(*) FROM habitat_df').fetchone()[0],
			'habitat_taxa': db.execute('SELECT COUNT(DISTINCT gbif_id) FROM habitat_df').fetchone()[0],
			'centroid_rows': db.execute('SELECT COUNT(*) FROM centroid_df').fetchone()[0],
			'elev_bin_rows': db.execute('SELECT COUNT(*) FROM elevation_bins_df').fetchone()[0],
			'elev_median_rows': db.execute('SELECT COUNT(*) FROM elevation_medians_df').fetchone()[0],
		}
		# Log source cardinalities
		log_line(log_path, f"SOURCE {json.dumps(source_stats)}")
		# Run baseline variant A
		variant_a = run_variant_a(db, log_path)
		# Run alternative variant C
		variant_c = run_variant_c(db, log_path)
		# Run alternative variant D
		variant_d = run_variant_d(db, log_path)
		# Collect output row counts for quick sanity
		result_rows = {
			'rows_a': db.execute('SELECT COUNT(*) FROM geo_a').fetchone()[0],
			'rows_c': db.execute('SELECT COUNT(*) FROM geo_c').fetchone()[0],
			'rows_d': db.execute('SELECT COUNT(*) FROM geo_d').fetchone()[0],
		}
		# Build final summary payload
		summary = {
			'release': args.release,
			'modulo': args.modulo,
			'remainder': args.remainder,
			'source': source_stats,
			'variant_a': variant_a,
			'variant_c': variant_c,
			'variant_d': variant_d,
			'rows': result_rows,
		}
		# Log final summary JSON for machine parsing
		log_line(log_path, 'SUMMARY ' + json.dumps(summary))
		# Also print pretty summary to stdout
		print(json.dumps(summary, indent=2))

# Main guard
if __name__ == '__main__':
	# Run cli entrypoint with crash logging
	try:
		# Execute script main
		main()
	# Catch any unhandled error to preserve traceback in log-friendly stdout
	except Exception:
		# Print traceback for debugging
		traceback.print_exc()
		# Re-raise failure for non-zero exit code
		raise
