# 10 upper-band habitat consolidation benchmark
# Purpose: compare tile consolidation strategies for very large habitat maps.
# Scope: performance + output-shape comparison, not production mutation.
# Notes:
# - Input is staged habitat parquet with columns: gbif_id, tile_id, center_lng, center_lat, count.
# - Consolidation is applied only to taxa above tile-cap threshold.
# - This benchmark keeps total occurrence mass conserved per approach.

# Load CLI parsing
import argparse
# Load filesystem helpers
import os
# Load wall-clock timing helpers
import time
# Load JSON output formatting
import json
# Load duckdb SQL engine
import duckdb

# Keep tile axis count aligned with geo pipeline zoom-10 encoding
TILE_COUNT = 1024

# Time one SQL statement and return elapsed seconds
def timed_execute(db: duckdb.DuckDBPyConnection, label: str, sql: str, params: list | None = None) -> float:
	# Capture start time for this step
	t0 = time.perf_counter()
	# Execute statement with optional parameters
	db.execute(sql, params or [])
	# Compute elapsed seconds for this step
	elapsed = time.perf_counter() - t0
	# Print per-step timing for operator visibility
	print(f"ABTEST : {label} {elapsed:.3f}s")
	# Return elapsed seconds
	return elapsed

# Build optional deterministic partition filter for quick sample runs
def partition_predicate(alias: str, modulo: int, remainder: int) -> str:
	# Return always-true predicate when no partitioning is requested
	if modulo <= 1: return 'TRUE'
	# Return hash predicate against gbif_id for deterministic slices
	return f"abs(hash({alias}.gbif_id)) % {modulo} = {remainder}"

# Collect output summary metrics from one result table
def collect_metrics(db: duckdb.DuckDBPyConnection, table_name: str, cap: int) -> dict:
	# Count all output rows in consolidated table
	rows = db.execute(f"SELECT COUNT(*) FROM {table_name}").fetchone()[0]
	# Count represented taxa in consolidated table
	taxa = db.execute(f"SELECT COUNT(DISTINCT gbif_id) FROM {table_name}").fetchone()[0]
	# Compute max tile count per taxon after consolidation
	max_tiles = db.execute(f"SELECT COALESCE(MAX(n_tiles),0) FROM (SELECT gbif_id, COUNT(*) AS n_tiles FROM {table_name} GROUP BY gbif_id)").fetchone()[0]
	# Count taxa still above cap after consolidation
	over_cap = db.execute(
		f"""
		SELECT COUNT(*)
		FROM (
			SELECT gbif_id, COUNT(*) AS n_tiles
			FROM {table_name}
			GROUP BY gbif_id
		) t
		WHERE t.n_tiles > {cap}
		"""
	).fetchone()[0]
	# Sum occurrence mass for conservation checks
	total_mass = db.execute(f"SELECT COALESCE(SUM(count),0) FROM {table_name}").fetchone()[0]
	# Return metric map
	return {
		'rows': int(rows),
		'taxa': int(taxa),
		'max_tiles_per_taxon': int(max_tiles),
		'taxa_over_cap': int(over_cap),
		'total_occ_mass': int(total_mass),
	}

# Run one approach in an isolated in-memory DuckDB and return timings + metrics
def run_approach(habitat_path: str, tmp_dir: str, cap: int, top_keep: int, sparse_occ: int, modulo: int, remainder: int, approach: str) -> dict:
	# Open isolated in-memory DB for fair per-approach timings
	with duckdb.connect(':memory:') as db:
		# Route spill files to importer temp directory
		db.execute(f"SET temp_directory = '{tmp_dir}'")
		# Allow DuckDB to optimize hash/group operators aggressively
		db.execute("SET preserve_insertion_order = false")
		# Keep large Arrow buffers enabled for wide JSON-safe behavior
		db.execute("SET arrow_large_buffer_size=true")
		# Build deterministic partition predicate for source load
		pred = partition_predicate('h', modulo, remainder)
		# Track step timings for this approach
		timings = {}
		# Load input habitat rows (optionally partitioned)
		timings['load_input'] = timed_execute(db, f"{approach} load input", f"""
			CREATE TEMP TABLE habitat_df AS
			SELECT h.gbif_id, h.tile_id, h.center_lng, h.center_lat, h.count
			FROM read_parquet(?) h
			WHERE {pred};
		""", [habitat_path])
		# Build per-taxon tile stats used by all approaches
		timings['tile_stats'] = timed_execute(db, f"{approach} tile stats", """
			CREATE TEMP TABLE taxa_stats AS
			SELECT gbif_id, COUNT(*) AS tile_count
			FROM habitat_df
			GROUP BY gbif_id;
		""")
		# Build heavy-taxa set above requested tile cap
		timings['heavy_taxa'] = timed_execute(db, f"{approach} heavy taxa", f"""
			CREATE TEMP TABLE heavy_taxa AS
			SELECT gbif_id, tile_count
			FROM taxa_stats
			WHERE tile_count > {cap};
		""")
		# Compute baseline input metrics before consolidation
		input_metrics = collect_metrics(db, 'habitat_df', cap)

		# Apply approach A: adaptive coarsen all heavy taxa tiles
		if approach == 'adaptive_all':
			# Build consolidated output with full-heavy adaptive coarsening
			timings['consolidate'] = timed_execute(db, 'adaptive_all consolidate', f"""
				CREATE TABLE out_habitat AS
				WITH heavy_cfg AS (
					SELECT
						gbif_id,
						CAST(greatest(1, ceil(ln(tile_count::DOUBLE / {cap}::DOUBLE) / ln(4.0))) AS INTEGER) AS shift
					FROM heavy_taxa
				),
				non_heavy AS (
					SELECT h.gbif_id, h.tile_id, h.center_lng, h.center_lat, h.count
					FROM habitat_df h
					LEFT JOIN heavy_taxa t ON h.gbif_id = t.gbif_id
					WHERE t.gbif_id IS NULL
				),
				heavy_coarse AS (
					SELECT
						h.gbif_id,
						CAST(
							floor(floor(h.tile_id / {TILE_COUNT}) / pow(2, c.shift)) * {TILE_COUNT}
							+ floor((h.tile_id % {TILE_COUNT}) / pow(2, c.shift))
						AS UINTEGER) AS tile_id,
						sum(h.center_lng * h.count) / sum(h.count) AS center_lng,
						sum(h.center_lat * h.count) / sum(h.count) AS center_lat,
						sum(h.count) AS count
					FROM habitat_df h
					JOIN heavy_cfg c ON h.gbif_id = c.gbif_id
					GROUP BY 1,2
				)
				SELECT * FROM non_heavy
				UNION ALL
				SELECT * FROM heavy_coarse;
			""")
		# Apply approach B: keep dense and sparse extremes, coarsen only middle band
		elif approach == 'keep_extremes_coarse_mid':
			# Build consolidated output with nuanced split for heavy taxa
			timings['consolidate'] = timed_execute(db, 'keep_extremes_coarse_mid consolidate', f"""
				CREATE TABLE out_habitat AS
				WITH ranked_heavy AS (
					SELECT
						h.gbif_id,
						h.tile_id,
						h.center_lng,
						h.center_lat,
						h.count,
						row_number() OVER (PARTITION BY h.gbif_id ORDER BY h.count DESC, h.tile_id ASC) AS rn
					FROM habitat_df h
					JOIN heavy_taxa t ON h.gbif_id = t.gbif_id
				),
				heavy_split AS (
					SELECT
						gbif_id,
						tile_id,
						center_lng,
						center_lat,
						count,
						rn <= {top_keep} OR count <= {sparse_occ} AS keep_direct
					FROM ranked_heavy
				),
				heavy_counts AS (
					SELECT
						gbif_id,
						sum(CASE WHEN keep_direct THEN 1 ELSE 0 END) AS keep_tiles,
						sum(CASE WHEN keep_direct THEN 0 ELSE 1 END) AS middle_tiles
					FROM heavy_split
					GROUP BY gbif_id
				),
				heavy_cfg AS (
					SELECT
						gbif_id,
						keep_tiles,
						middle_tiles,
						CAST(greatest(1, ceil(ln(greatest(1, middle_tiles)::DOUBLE / greatest(64, {cap} - keep_tiles)::DOUBLE) / ln(4.0))) AS INTEGER) AS shift
					FROM heavy_counts
				),
				non_heavy AS (
					SELECT h.gbif_id, h.tile_id, h.center_lng, h.center_lat, h.count
					FROM habitat_df h
					LEFT JOIN heavy_taxa t ON h.gbif_id = t.gbif_id
					WHERE t.gbif_id IS NULL
				),
				heavy_kept AS (
					SELECT gbif_id, tile_id, center_lng, center_lat, count
					FROM heavy_split
					WHERE keep_direct
				),
				heavy_middle_coarse AS (
					SELECT
						h.gbif_id,
						CAST(
							floor(floor(h.tile_id / {TILE_COUNT}) / pow(2, c.shift)) * {TILE_COUNT}
							+ floor((h.tile_id % {TILE_COUNT}) / pow(2, c.shift))
						AS UINTEGER) AS tile_id,
						sum(h.center_lng * h.count) / sum(h.count) AS center_lng,
						sum(h.center_lat * h.count) / sum(h.count) AS center_lat,
						sum(h.count) AS count
					FROM heavy_split h
					JOIN heavy_cfg c ON h.gbif_id = c.gbif_id
					WHERE NOT h.keep_direct
					GROUP BY 1,2
				)
				SELECT * FROM non_heavy
				UNION ALL
				SELECT * FROM heavy_kept
				UNION ALL
				SELECT * FROM heavy_middle_coarse;
			""")
		# Apply approach C: keep only top-mass direct, coarsen all heavy tail
		elif approach == 'keep_top_coarse_tail':
			# Build consolidated output with top tiles preserved and tail adaptively coarsened
			timings['consolidate'] = timed_execute(db, 'keep_top_coarse_tail consolidate', f"""
				CREATE TABLE out_habitat AS
				WITH ranked_heavy AS (
					SELECT
						h.gbif_id,
						h.tile_id,
						h.center_lng,
						h.center_lat,
						h.count,
						row_number() OVER (PARTITION BY h.gbif_id ORDER BY h.count DESC, h.tile_id ASC) AS rn
					FROM habitat_df h
					JOIN heavy_taxa t ON h.gbif_id = t.gbif_id
				),
				heavy_counts AS (
					SELECT
						gbif_id,
						sum(CASE WHEN rn <= {top_keep} THEN 1 ELSE 0 END) AS keep_tiles,
						sum(CASE WHEN rn <= {top_keep} THEN 0 ELSE 1 END) AS tail_tiles
					FROM ranked_heavy
					GROUP BY gbif_id
				),
				heavy_cfg AS (
					SELECT
						gbif_id,
						CAST(greatest(1, ceil(ln(greatest(1, tail_tiles)::DOUBLE / greatest(64, {cap} - keep_tiles)::DOUBLE) / ln(4.0))) AS INTEGER) AS shift
					FROM heavy_counts
				),
				non_heavy AS (
					SELECT h.gbif_id, h.tile_id, h.center_lng, h.center_lat, h.count
					FROM habitat_df h
					LEFT JOIN heavy_taxa t ON h.gbif_id = t.gbif_id
					WHERE t.gbif_id IS NULL
				),
				heavy_kept AS (
					SELECT gbif_id, tile_id, center_lng, center_lat, count
					FROM ranked_heavy
					WHERE rn <= {top_keep}
				),
				heavy_tail_coarse AS (
					SELECT
						h.gbif_id,
						CAST(
							floor(floor(h.tile_id / {TILE_COUNT}) / pow(2, c.shift)) * {TILE_COUNT}
							+ floor((h.tile_id % {TILE_COUNT}) / pow(2, c.shift))
						AS UINTEGER) AS tile_id,
						sum(h.center_lng * h.count) / sum(h.count) AS center_lng,
						sum(h.center_lat * h.count) / sum(h.count) AS center_lat,
						sum(h.count) AS count
					FROM ranked_heavy h
					JOIN heavy_cfg c ON h.gbif_id = c.gbif_id
					WHERE h.rn > {top_keep}
					GROUP BY 1,2
				)
				SELECT * FROM non_heavy
				UNION ALL
				SELECT * FROM heavy_kept
				UNION ALL
				SELECT * FROM heavy_tail_coarse;
			""")
		# Abort for unsupported approach values
		else:
			# Raise explicit value error for bad approach names
			raise ValueError(f"Unsupported approach: {approach}")

		# Collect output metrics from consolidated table
		output_metrics = collect_metrics(db, 'out_habitat', cap)
		# Return full run summary for this approach
		return {
			'approach': approach,
			'cap': cap,
			'top_keep': top_keep,
			'sparse_occ': sparse_occ,
			'modulo': modulo,
			'remainder': remainder,
			'timings': timings,
			'input': input_metrics,
			'output': output_metrics,
			'consolidation_ratio': round(output_metrics['rows'] / max(1, input_metrics['rows']), 6),
		}

# CLI entrypoint
def main():
	# Build parser
	parser = argparse.ArgumentParser()
	# Select release token used in staged dump filename
	parser.add_argument('--release', default='20260401-7e70f9623fe7')
	# Select directory containing staged habitat dump
	parser.add_argument('--dump-dir', default='data/temp')
	# Select tile cap above which taxa are consolidated
	parser.add_argument('--cap', type=int, default=20000)
	# Select direct-keep top tiles for nuanced approaches
	parser.add_argument('--top-keep', type=int, default=1500)
	# Select sparse-tile keep threshold for nuanced approach B
	parser.add_argument('--sparse-occ', type=int, default=2)
	# Select deterministic partition modulus for quick runs
	parser.add_argument('--modulo', type=int, default=1)
	# Select deterministic partition remainder for quick runs
	parser.add_argument('--remainder', type=int, default=0)
	# Select approaches to run (comma-separated)
	parser.add_argument('--approaches', default='adaptive_all,keep_extremes_coarse_mid,keep_top_coarse_tail')
	# Parse args
	args = parser.parse_args()

	# Resolve staged habitat dump path
	habitat_path = os.path.join(args.dump_dir, f'geo_abtest_habitat_{args.release}.parquet')
	# Abort when staged habitat dump is missing
	if not os.path.isfile(habitat_path): raise FileNotFoundError(f"Missing habitat dump: {habitat_path}")
	# Parse requested approach list
	approaches = [a.strip() for a in args.approaches.split(',') if a.strip()]
	# Hold results for all requested approaches
	results = []
	# Run each requested approach in isolation
	for approach in approaches:
		# Announce approach run start
		print(f"ABTEST : Running {approach}")
		# Execute one approach and append result summary
		results.append(run_approach(
			habitat_path=habitat_path,
			tmp_dir=args.dump_dir,
			cap=args.cap,
			top_keep=args.top_keep,
			sparse_occ=args.sparse_occ,
			modulo=args.modulo,
			remainder=args.remainder,
			approach=approach,
		))
	# Emit JSON summary for downstream diffing
	print(json.dumps({
		'release': args.release,
		'habitat_path': habitat_path,
		'results': results,
	}, indent=2))

# Run CLI main
if __name__ == '__main__':
	# Execute script entrypoint
	main()
