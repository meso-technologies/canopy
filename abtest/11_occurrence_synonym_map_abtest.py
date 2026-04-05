# 11 occurrence synonym mapping and write benchmark
# Purpose:
# - Run GBIF occurrence extraction exactly once into a reusable baseline parquet.
# - Keep historical synonym-mapping variants A/C documented, but run winner B by default.
# - Benchmark three write strategies after B mapping on the same mapped table.
#
# Current measured mapping results on full baseline (501,954,619 rows):
# - a_update_from_map: 34.205s
# - b_scalar_subquery_update: 18.929s  <-- winner
# - c_ctas_join_swap: 68.412s
#
# Usage examples:
# - Extract once + run winner mapping B and write variants:
#   uv run python abtest/11_occurrence_synonym_map_abtest.py
#
# - Reuse existing extracted baseline and rerun winner mapping + write variants:
#   uv run python abtest/11_occurrence_synonym_map_abtest.py
#
# - Use custom memory cap and thread count:
#   uv run python abtest/11_occurrence_synonym_map_abtest.py --memory-limit 64GB --threads 16

# Load filesystem helpers
import os
# Load zip handling for occurrences bootstrap file
import zipfile
# Load in-memory byte stream helper for parquet chunks
import io
# Load timing helpers for benchmark durations
import time
# Load json serialization for benchmark summary output
import json
# Load timestamp helper for result filenames
from datetime import datetime
# Load pathlib for robust path joins
from pathlib import Path

# Load duckdb for extraction and benchmark SQL
import duckdb
# Load polars for reading parquet chunks from zip members
import polars as pl

# Resolve canopy directory from this script location
CANOPY_DIR = Path(__file__).resolve().parents[1]
# Resolve canopy data directory
DATA_DIR = CANOPY_DIR / 'data'
# Resolve canopy temp directory
TMP_DIR = DATA_DIR / 'temp'
# Resolve canopy geo directory
GEO_DIR = DATA_DIR / 'geo'
# Resolve processed directory for GBIF backbone parquet
PROCESSED_DIR = DATA_DIR / 'processed'

# Resolve source bootstrap zip path expected by occurrences pipeline
OCC_ZIP_PATH = TMP_DIR / 'occurrences.zip'
# Resolve reusable extracted baseline parquet path for this abtest
EXTRACTED_BASELINE_PATH = TMP_DIR / 'abtest11_occurrences_extracted_base.parquet'
# Resolve benchmark result json path
RESULT_PATH = TMP_DIR / f"abtest11_results_{datetime.now().strftime('%Y%m%d_%H%M%S')}.json"
# Keep benchmark memory limit explicit for repeatable local comparisons
BENCH_MEMORY_LIMIT = '64GB'
# Keep benchmark worker count explicit but adaptive to host capacity
BENCH_THREADS = max(4, os.cpu_count() or 8)

# Main orchestrator kept near the top for fast readability.
# Flow:
# 1) Prepare DB and reusable extraction baseline.
# 2) Run mapping benchmark (winner B only).
#    Historical results: A=34.205s, B=18.929s, C=68.412s.
# 3) Run write A/B/C benchmarks on the mapped table.
def main():
	# Ensure canopy temp directory exists
	TMP_DIR.mkdir(parents=True, exist_ok=True)
	# Open benchmark connection
	db = open_db(BENCH_MEMORY_LIMIT, BENCH_THREADS)
	# Extract once unless reusable baseline already exists
	extract_once_if_needed(db, force_extract=False)
	# Abort when baseline is still missing
	if not EXTRACTED_BASELINE_PATH.is_file(): raise FileNotFoundError(f'Missing extracted baseline {EXTRACTED_BASELINE_PATH}')
	# Resolve latest processed GBIF parquet for synonym map
	gbif_path = latest_gbif_processed()
	# Build mapping table once for all variants
	build_synonym_map(db, gbif_path)
	# Capture benchmark start timestamp
	bench_start = time.perf_counter()
	# Keep historical mapping variants for reference only
	# Historical result: A update-from-map = 34.205s mapped=46,183,819
	# res_a = benchmark_variant_a(db)
	# Winner result: B scalar-subquery-update = 18.929s mapped=46,183,819
	res_b = benchmark_variant_b(db)
	# Historical result: C ctas-join-swap = 68.412s mapped=46,183,819
	# res_c = benchmark_variant_c(db)
	# Print winner mapping timing
	print(f"ABTEST11 {res_b['name']} {res_b['seconds']:.3f}s mapped={res_b['mapped_rows']:,}", flush=True)
	# Run write variant A on mapped table
	# Result placeholder: fill after first write benchmark run
	write_a = benchmark_write_a(db)
	# Print write variant A timing and size
	print(f"ABTEST11 {write_a['name']} {write_a['seconds']:.3f}s bytes={write_a['bytes']:,}", flush=True)
	# Run write variant B on mapped table
	# Result placeholder: fill after first write benchmark run
	write_b = benchmark_write_b(db)
	# Print write variant B timing and size
	print(f"ABTEST11 {write_b['name']} {write_b['seconds']:.3f}s bytes={write_b['bytes']:,}", flush=True)
	# Run write variant C on mapped table
	# Result placeholder: fill after first write benchmark run
	write_c = benchmark_write_c(db)
	# Print write variant C timing and size
	print(f"ABTEST11 {write_c['name']} {write_c['seconds']:.3f}s bytes={write_c['bytes']:,}", flush=True)
	# Compute total benchmark runtime excluding optional extraction phase
	bench_elapsed = time.perf_counter() - bench_start
	# Build summary payload
	summary = {
		# Include benchmark execution context
		'context': {
			'created_at_utc': datetime.now().astimezone().isoformat(),
			'memory_limit': BENCH_MEMORY_LIMIT,
			'threads': BENCH_THREADS,
			'baseline_parquet': EXTRACTED_BASELINE_PATH.as_posix(),
			'gbif_parquet': gbif_path.as_posix(),
		},
		# Keep historical mapping timing references for documentation
		'historical_mapping': {
			'a_update_from_map_seconds': 34.205,
			'b_scalar_subquery_update_seconds': 18.929,
			'c_ctas_join_swap_seconds': 68.412,
		},
		# Include active run mapping winner result
		'mapping': res_b,
		# Include write variant benchmark results
		'writes': [write_a, write_b, write_c],
		# Include benchmark wall time
		'benchmark_seconds': bench_elapsed,
	}
	# Persist json result for post-run comparison
	with open(RESULT_PATH, 'w', encoding='utf-8') as file:
		# Write formatted json payload
		json.dump(summary, file, indent=2)
	# Print result path for operator convenience
	print(f'ABTEST11 results written to {RESULT_PATH}', flush=True)

# Build DuckDB connection with canopy-appropriate runtime settings
def open_db(memory_limit: str, threads: int) -> duckdb.DuckDBPyConnection:
	# Create isolated in-memory DuckDB connection
	db = duckdb.connect(':memory:')
	# Route DuckDB temp spill files to canopy temp directory
	db.execute(f"SET temp_directory = '{TMP_DIR.as_posix()}'")
	# Keep mapping tests bounded to requested memory cap
	db.execute(f"SET memory_limit = '{memory_limit}'")
	# Use requested execution thread count
	db.execute(f"SET threads = {threads}")
	# Return configured connection
	return db

# Resolve newest processed GBIF parquet needed for synonym mapping
def latest_gbif_processed() -> Path:
	# List processed gbif parquet candidates
	candidates = sorted(PROCESSED_DIR.glob('gbif.*.parquet'))
	# Abort when no GBIF processed parquet exists
	if not candidates: raise FileNotFoundError(f'No processed gbif parquet found in {PROCESSED_DIR}')
	# Return newest filename by lexical timestamp ordering
	return candidates[-1]

# Extract occurrences once into a reusable baseline parquet
# Baseline keeps only columns needed for mapping benchmarks
def extract_once_if_needed(db: duckdb.DuckDBPyConnection, force_extract: bool):
	# Skip extraction when baseline already exists and force flag is not set
	if EXTRACTED_BASELINE_PATH.is_file() and not force_extract:
		# Announce reuse path
		print(f'ABTEST11 reuse extracted baseline {EXTRACTED_BASELINE_PATH}', flush=True)
		# Stop early
		return
	# Abort if bootstrap occurrences zip is missing
	if not OCC_ZIP_PATH.is_file(): raise FileNotFoundError(f'Missing bootstrap zip {OCC_ZIP_PATH}')
	# Announce extraction start
	print('ABTEST11 extraction start', flush=True)
	# Load spatial extension for geometry point creation
	db.execute('INSTALL spatial; LOAD spatial;')
	# Create extraction target table
	db.execute("""
		CREATE TABLE occ_extract (
			id UBIGINT,
			taxon_raw UINTEGER,
			location GEOMETRY,
			elevation SMALLINT,
			spatial_issue BOOLEAN DEFAULT FALSE
		);
	""")
	# Open bootstrap zip with parquet chunk files
	with zipfile.ZipFile(OCC_ZIP_PATH, 'r') as zip_file:
		# Track current file counter for progress logging
		counter = 0
		# Track total member count for progress logging
		total = len(zip_file.filelist)
		# Iterate through all members in zip archive
		for member in zip_file.filelist:
			# Skip empty members
			if member.file_size == 0: continue
			# Increase processed member counter
			counter += 1
			# Read member parquet with polars from in-memory bytes
			df = pl.read_parquet(io.BytesIO(zip_file.read(member.filename)))
			# Insert filtered rows into extraction table
			db.execute("""
				INSERT INTO occ_extract BY NAME
				SELECT
					gbifid AS id,
					CAST(taxonkey AS UINTEGER) AS taxon_raw,
					ST_Point(ROUND(decimallongitude, 3), ROUND(decimallatitude, 3)) AS location,
					COALESCE(elevation, depth * -1) AS elevation,
					list_contains(issue, 'HAS_GEOSPATIAL_ISSUE') AS spatial_issue
				FROM df
				WHERE kingdom IN ('Plantae','Fungi','Incertae sedis')
				AND decimallatitude IS NOT NULL
				AND decimallongitude IS NOT NULL
				AND NOT ST_Equals(ST_Point(decimallongitude, decimallatitude), ST_Point(0, 0));
			""")
			# Emit periodic progress update every 250 files
			if counter % 250 == 0:
				# Fetch running row count for progress visibility
				rows = db.execute('SELECT COUNT(*) FROM occ_extract').fetchone()[0]
				# Print extraction progress line
				print(f'ABTEST11 extract progress {counter}/{total} files rows={rows:,}', flush=True)
	# Persist extracted baseline once for repeated mapping tests
	db.execute(f"COPY occ_extract TO '{EXTRACTED_BASELINE_PATH.as_posix()}'")
	# Print extraction completion summary
	rows = db.execute('SELECT COUNT(*) FROM occ_extract').fetchone()[0]
	print(f'ABTEST11 extraction done rows={rows:,} path={EXTRACTED_BASELINE_PATH}', flush=True)
	# Drop extraction working table to free memory before mapping tests
	db.execute('DROP TABLE occ_extract')

# Prepare compact unique synonym mapping table
# Uses accepted_raw from processed gbif parquet and groups by synonym key
def build_synonym_map(db: duckdb.DuckDBPyConnection, gbif_path: Path):
	# Drop prior map table if present
	db.execute('DROP TABLE IF EXISTS gbif_synonym_map')
	# Build one-row-per-synonym mapping table
	db.execute(f"""
		CREATE TABLE gbif_synonym_map AS
		SELECT
			CAST(id_raw AS UINTEGER) AS synonym_key,
			MIN(CAST(accepted_raw AS UINTEGER)) AS accepted_key
		FROM read_parquet('{gbif_path.as_posix()}')
		WHERE status_clean = 'synonym' AND accepted_raw IS NOT NULL
		GROUP BY 1;
	""")

# Load extracted baseline into a fresh per-variant working table
def load_variant_work_table(db: duckdb.DuckDBPyConnection):
	# Drop previous working table if present
	db.execute('DROP TABLE IF EXISTS occ_work')
	# Build fresh working table from extracted baseline
	db.execute(f"""
		CREATE TABLE occ_work AS
		SELECT
			id,
			taxon_raw,
			CAST(taxon_raw AS UINTEGER) AS taxon,
			CAST(NULL AS UINTEGER) AS synonym_for,
			location,
			elevation,
			spatial_issue
		FROM read_parquet('{EXTRACTED_BASELINE_PATH.as_posix()}');
	""")

# Benchmark variant A: update from mapping table (current production-style approach)
def benchmark_variant_a(db: duckdb.DuckDBPyConnection) -> dict:
	# Load fresh variant working table
	load_variant_work_table(db)
	# Capture start time
	t0 = time.perf_counter()
	# Update synonym rows from mapping table
	db.execute("""
		UPDATE occ_work o
		SET
			synonym_for = m.accepted_key,
			taxon = m.accepted_key
		FROM gbif_synonym_map m
		WHERE o.taxon_raw = m.synonym_key;
	""")
	# Compute elapsed seconds
	elapsed = time.perf_counter() - t0
	# Collect mapped row count
	mapped_rows = db.execute('SELECT COUNT(*) FROM occ_work WHERE synonym_for IS NOT NULL').fetchone()[0]
	# Return benchmark payload
	return {'name': 'a_update_from_map', 'seconds': elapsed, 'mapped_rows': mapped_rows}

# Benchmark variant B: scalar subquery mapping update
# This tests whether correlated subquery planning is faster/slower than UPDATE..FROM
# on this data shape
# (expected slower in many engines, but measured here for certainty)
def benchmark_variant_b(db: duckdb.DuckDBPyConnection) -> dict:
	# Load fresh variant working table
	load_variant_work_table(db)
	# Capture start time
	t0 = time.perf_counter()
	# Update synonym rows through scalar subquery lookups
	db.execute("""
		UPDATE occ_work
		SET
			synonym_for = (
				SELECT m.accepted_key
				FROM gbif_synonym_map m
				WHERE m.synonym_key = occ_work.taxon_raw
			),
			taxon = COALESCE((
				SELECT m.accepted_key
				FROM gbif_synonym_map m
				WHERE m.synonym_key = occ_work.taxon_raw
			), taxon_raw)
		WHERE taxon_raw IN (SELECT synonym_key FROM gbif_synonym_map);
	""")
	# Compute elapsed seconds
	elapsed = time.perf_counter() - t0
	# Collect mapped row count
	mapped_rows = db.execute('SELECT COUNT(*) FROM occ_work WHERE synonym_for IS NOT NULL').fetchone()[0]
	# Return benchmark payload
	return {'name': 'b_scalar_subquery_update', 'seconds': elapsed, 'mapped_rows': mapped_rows}

# Benchmark variant C: CTAS join rewrite
# This avoids UPDATE writes and builds a replacement table via one left join scan
# then swaps table names.
# Historical only (kept for reference, not run by default).
def benchmark_variant_c(db: duckdb.DuckDBPyConnection) -> dict:
	# Load fresh variant working table
	load_variant_work_table(db)
	# Capture start time
	t0 = time.perf_counter()
	# Build mapped replacement table with one left join pass
	db.execute("""
		CREATE TABLE occ_work_mapped AS
		SELECT
			o.id,
			o.taxon_raw,
			COALESCE(m.accepted_key, o.taxon_raw) AS taxon,
			m.accepted_key AS synonym_for,
			o.location,
			o.elevation,
			o.spatial_issue
		FROM occ_work o
		LEFT JOIN gbif_synonym_map m ON o.taxon_raw = m.synonym_key;
	""")
	# Swap mapped table into working name
	db.execute('DROP TABLE occ_work')
	# Rename mapped table for consistent validation query
	db.execute('ALTER TABLE occ_work_mapped RENAME TO occ_work')
	# Compute elapsed seconds
	elapsed = time.perf_counter() - t0
	# Collect mapped row count
	mapped_rows = db.execute('SELECT COUNT(*) FROM occ_work WHERE synonym_for IS NOT NULL').fetchone()[0]
	# Return benchmark payload
	return {'name': 'c_ctas_join_swap', 'seconds': elapsed, 'mapped_rows': mapped_rows}

# Benchmark write variant A: default parquet copy settings
def benchmark_write_a(db: duckdb.DuckDBPyConnection) -> dict:
	# Resolve output path for variant A
	out_path = TMP_DIR / 'abtest11_occurrences_write_a_default.parquet'
	# Remove prior output file if present
	if out_path.is_file(): out_path.unlink()
	# Capture write start time
	t0 = time.perf_counter()
	# Run default parquet copy
	db.execute(f"COPY occ_work TO '{out_path.as_posix()}'")
	# Compute elapsed seconds
	elapsed = time.perf_counter() - t0
	# Read file size in bytes for throughput comparison
	size = out_path.stat().st_size if out_path.is_file() else 0
	# Return benchmark payload
	return {'name': 'write_a_default_copy', 'seconds': elapsed, 'bytes': size, 'path': out_path.as_posix()}

# Benchmark write variant B: snappy compression for faster writes
def benchmark_write_b(db: duckdb.DuckDBPyConnection) -> dict:
	# Resolve output path for variant B
	out_path = TMP_DIR / 'abtest11_occurrences_write_b_snappy.parquet'
	# Remove prior output file if present
	if out_path.is_file(): out_path.unlink()
	# Capture write start time
	t0 = time.perf_counter()
	# Run parquet copy with snappy compression
	db.execute(f"COPY occ_work TO '{out_path.as_posix()}' (FORMAT PARQUET, COMPRESSION snappy)")
	# Compute elapsed seconds
	elapsed = time.perf_counter() - t0
	# Read file size in bytes for throughput comparison
	size = out_path.stat().st_size if out_path.is_file() else 0
	# Return benchmark payload
	return {'name': 'write_b_snappy', 'seconds': elapsed, 'bytes': size, 'path': out_path.as_posix()}

# Benchmark write variant C: zstd compression tuned for smaller files
def benchmark_write_c(db: duckdb.DuckDBPyConnection) -> dict:
	# Resolve output path for variant C
	out_path = TMP_DIR / 'abtest11_occurrences_write_c_zstd.parquet'
	# Remove prior output file if present
	if out_path.is_file(): out_path.unlink()
	# Capture write start time
	t0 = time.perf_counter()
	# Run parquet copy with zstd compression
	db.execute(f"COPY occ_work TO '{out_path.as_posix()}' (FORMAT PARQUET, COMPRESSION zstd)")
	# Compute elapsed seconds
	elapsed = time.perf_counter() - t0
	# Read file size in bytes for throughput comparison
	size = out_path.stat().st_size if out_path.is_file() else 0
	# Return benchmark payload
	return {'name': 'write_c_zstd', 'seconds': elapsed, 'bytes': size, 'path': out_path.as_posix()}

# Run benchmark script when executed directly
if __name__ == '__main__':
	# Start main benchmark flow
	main()
