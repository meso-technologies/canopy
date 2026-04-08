# 12 full habitat rollup rewrite benchmark
# Purpose:
# - Recompute full upper-rank habitat rows for all taxa from low-rank habitat seeds.
# - Compare baseline behavior vs guarded behavior in the same shape as production rollup logic.
# - Write full output parquet files so downstream checks can inspect real resulting rows.
#
# Variant A baseline:
# - Merge by tile id at every recursive parent step.
# - Use weighted center for merged rows.
#
# Variant B guarded_anchor:
# - Once a propagated row exceeds lock_occ, keep it on a unique merge key forever.
# - This prevents repeated collapse of already-large rows with sibling rows.
# - Use densest-child anchor center for grouped rows (less synthetic than weighted center).
#
# Output files:
# - data/temp/abtest12_full_baseline.parquet
# - data/temp/abtest12_full_guarded_anchor.parquet

# Load cli parser
import argparse
# Load filesystem helpers
import os
# Load glob helpers
import glob
# Load timing helpers
import time
# Load json serializer for summary output
import json
# Load duckdb sql engine
import duckdb

# Keep tile axis count aligned with production zoom-10 encoding
TILE_COUNT = 1024

# Time one sql statement and print elapsed seconds.
def timed_execute(db: duckdb.DuckDBPyConnection, label: str, sql: str, params: list | None = None) -> float:
	# Capture step start time
	t0 = time.perf_counter()
	# Execute sql with optional parameters
	db.execute(sql, params or [])
	# Compute elapsed duration
	elapsed = time.perf_counter() - t0
	# Print operator timing line
	print(f'ABTEST12 : {label} {elapsed:.3f}s')
	# Return elapsed duration
	return elapsed

# Resolve canonical release parquet path when not explicitly provided.
def resolve_release_parquet(explicit_path: str | None) -> str:
	# Use explicit path when provided
	if explicit_path and os.path.isfile(explicit_path): return explicit_path
	# Discover all release parquet files
	candidates = sorted(glob.glob('data/releases/*/*.parquet'))
	# Hold canonical release parquet files only
	release_candidates = []
	# Iterate discovered files
	for path in candidates:
		# Resolve parent release directory name
		release_dir = os.path.basename(os.path.dirname(path))
		# Keep only canonical release parquet in each release directory
		if os.path.basename(path) == f'{release_dir}.parquet': release_candidates.append(path)
	# Abort when no release parquet found
	if not release_candidates: raise FileNotFoundError('No canonical release parquet found under data/releases/*/*.parquet')
	# Return newest lexical release parquet path
	return release_candidates[-1]

# Open in-memory duckdb with safe spill path.
def open_db(temp_dir: str) -> duckdb.DuckDBPyConnection:
	# Open in-memory database
	db = duckdb.connect(':memory:')
	# Route spill files into canopy temp directory
	db.execute(f"SET temp_directory = '{temp_dir}'")
	# Allow hash/group optimizations
	db.execute('SET preserve_insertion_order = false')
	# Keep large arrow buffer safety
	db.execute('SET arrow_large_buffer_size=true')
	# Return configured connection
	return db

# Load accepted taxonomy and parent edges once.
def prepare_taxonomy(db: duckdb.DuckDBPyConnection, release_parquet: str, seed_ranks: list[str]):
	# Materialize accepted taxa with rank and parent pointer
	timed_execute(db, 'load taxa', """
		CREATE TEMP TABLE taxa AS
		SELECT gbif_id, id_meso, parent_consensus, rank_consensus, lower(name_consensus) AS name_consensus
		FROM read_parquet(?)
		WHERE accepted AND gbif_id IS NOT NULL;
	""", [release_parquet])
	# Build child->parent edges with parent rank attached
	timed_execute(db, 'build parentage', """
		CREATE TEMP TABLE parentage AS
		SELECT
			c.gbif_id,
			p.gbif_id AS parent_gbif,
			p.rank_consensus AS parent_rank
		FROM taxa c
		JOIN taxa p ON c.parent_consensus = p.id_meso
		WHERE c.gbif_id != p.gbif_id;
	""")
	# Build seed taxon id set used as rollup seeds
	seed_rank_sql = ','.join([f"'{rank}'" for rank in seed_ranks])
	timed_execute(db, 'build seed rank set', f"""
		CREATE TEMP TABLE low_rank_taxa AS
		SELECT gbif_id
		FROM taxa
		WHERE rank_consensus IN ({seed_rank_sql});
	""")
	# Build parent edge table used in recursive propagation above selected seed ranks
	seed_rank_sql = ','.join([f"'{rank}'" for rank in seed_ranks])
	timed_execute(db, 'build high edges', f"""
		CREATE TEMP TABLE high_parentage AS
		SELECT gbif_id, parent_gbif
		FROM parentage
		WHERE parent_rank NOT IN ({seed_rank_sql});
	""")

# Build seed habitat rows from either existing habitat centers or observed occurrence centers.
def prepare_seed_habitat(db: duckdb.DuckDBPyConnection, habitat_parquet: str, occurrences_parquet: str, seed_center_mode: str):
	# Keep existing tile-center seeds when requested
	if seed_center_mode == 'tile':
		# Keep only low-rank rows as seeds for upper-rank recomputation
		timed_execute(db, 'load seed habitat tile centers', """
			CREATE TEMP TABLE seed_habitat AS
			SELECT h.gbif_id, h.tile_id, h.center_lng, h.center_lat, h.count
			FROM read_parquet(?) h
			JOIN low_rank_taxa l ON h.gbif_id = l.gbif_id;
		""", [habitat_parquet])
	# Build organic seed centers from actual occurrences per taxon/tile
	elif seed_center_mode == 'observed':
		# Load spatial extension for point coordinate extraction
		timed_execute(db, 'load spatial extension', """
			INSTALL spatial;
			LOAD spatial;
		""")
		# Build seed habitat directly from occurrences grouped by taxon and tile id
		timed_execute(db, 'load seed habitat observed centers', f"""
			CREATE TEMP TABLE seed_habitat AS
			WITH occ AS (
				SELECT
					o.taxon AS gbif_id,
					ST_X(o.location) AS lng,
					ST_Y(o.location) AS lat
				FROM read_parquet(?) o
				JOIN low_rank_taxa l ON o.taxon = l.gbif_id
			),
			occ_tiles AS (
				SELECT
					gbif_id,
					CAST(
						floor(least(greatest(((lng + 180.0) / 360.0) * {TILE_COUNT}, 0.0), {TILE_COUNT - 1.0})) * {TILE_COUNT}
						+ floor(least(greatest(((1.0 - ln(tan(radians(least(greatest(lat, -85.05112878), 85.05112878))) + 1.0 / cos(radians(least(greatest(lat, -85.05112878), 85.05112878)))) / pi()) / 2.0) * {TILE_COUNT}, 0.0), {TILE_COUNT - 1.0}))
					AS UINTEGER) AS tile_id,
					lng,
					lat
				FROM occ
			)
			SELECT
				gbif_id,
				tile_id,
				(min(lng) + max(lng)) / 2.0 AS center_lng,
				(min(lat) + max(lat)) / 2.0 AS center_lat,
				count(*)::BIGINT AS count
			FROM occ_tiles
			GROUP BY gbif_id, tile_id;
		""", [occurrences_parquet])
	# Abort unsupported seed center modes
	else:
		# Raise explicit value error
		raise ValueError(f'Unsupported seed center mode: {seed_center_mode}')

# Build variant-specific initial frontier with merge keys.
def build_initial_frontier(db: duckdb.DuckDBPyConnection, variant: str, lock_occ: int):
	# Baseline frontier uses merge-by-tile only
	if variant == 'baseline':
		# Create baseline frontier from seed rows
		timed_execute(db, 'frontier seed baseline', """
			CREATE TEMP TABLE frontier AS
			SELECT
				gbif_id,
				tile_id,
				center_lng,
				center_lat,
				count,
				concat('m:', cast(tile_id AS VARCHAR)) AS merge_key
			FROM seed_habitat;
		""")
	# Guarded frontier uses unique merge keys once above threshold
	elif variant == 'guarded_anchor':
		# Create guarded frontier from seed rows
		timed_execute(db, 'frontier seed guarded', f"""
			CREATE TEMP TABLE frontier AS
			SELECT
				gbif_id,
				tile_id,
				center_lng,
				center_lat,
				count,
				CASE
					WHEN count > {lock_occ} THEN concat('u:', cast(tile_id AS VARCHAR), '#', cast(gbif_id AS VARCHAR))
					ELSE concat('m:', cast(tile_id AS VARCHAR))
				END AS merge_key
			FROM seed_habitat;
		""")
	# Abort unsupported variant names
	else:
		# Raise explicit value error
		raise ValueError(f'Unsupported variant: {variant}')
	# Seed empty accumulation table for propagated high-rank rows
	timed_execute(db, 'seed accum', """
		CREATE TEMP TABLE accum AS
		SELECT
			CAST(NULL AS UINTEGER) AS gbif_id,
			CAST(NULL AS UINTEGER) AS tile_id,
			CAST(NULL AS DOUBLE) AS center_lng,
			CAST(NULL AS DOUBLE) AS center_lat,
			CAST(NULL AS BIGINT) AS count,
			CAST(NULL AS VARCHAR) AS merge_key
		WHERE FALSE;
	""")

# Run recursive upper-rank propagation for one variant.
def run_recursive_rollup(db: duckdb.DuckDBPyConnection, variant: str, lock_occ: int):
	# Start recursion level counter
	level = 0
	# Iterate until no more parent rows are produced
	while True:
		# Build one propagated edge-expanded table
		timed_execute(db, f'{variant} level {level + 1} join', """
			CREATE OR REPLACE TEMP TABLE joined AS
			SELECT
				h.parent_gbif AS gbif_id,
				f.tile_id,
				f.center_lng,
				f.center_lat,
				f.count,
				f.merge_key
			FROM frontier f
			JOIN high_parentage h ON f.gbif_id = h.gbif_id;
		""")
		# Stop when no parent rows were produced
		row_count = db.execute('SELECT COUNT(*) FROM joined').fetchone()[0]
		# Break recursion on empty frontier
		if row_count == 0: break
		# Aggregate one level with variant-specific center/merge behavior
		if variant == 'baseline':
			# Baseline: merge by tile key and weighted centers
			timed_execute(db, f'{variant} level {level + 1} aggregate', """
				CREATE OR REPLACE TEMP TABLE next_frontier AS
				SELECT
					gbif_id,
					tile_id,
					sum(center_lng * count) / sum(count) AS center_lng,
					sum(center_lat * count) / sum(count) AS center_lat,
					sum(count) AS count,
					concat('m:', cast(tile_id AS VARCHAR)) AS merge_key
				FROM joined
				GROUP BY gbif_id, tile_id;
			""")
		elif variant == 'guarded_anchor':
			# Guarded: preserve unique keys once large; anchor centers to densest row in each group
			timed_execute(db, f'{variant} level {level + 1} aggregate', f"""
				CREATE OR REPLACE TEMP TABLE next_frontier AS
				WITH grouped AS (
					SELECT
						gbif_id,
						tile_id,
						merge_key,
						sum(count) AS count
					FROM joined
					GROUP BY gbif_id, tile_id, merge_key
				),
				anchor AS (
					SELECT
						gbif_id,
						tile_id,
						merge_key,
						center_lng,
						center_lat,
						row_number() OVER (PARTITION BY gbif_id, tile_id, merge_key ORDER BY count DESC, center_lng ASC, center_lat ASC) AS rn
					FROM joined
				)
				SELECT
					g.gbif_id,
					g.tile_id,
					a.center_lng,
					a.center_lat,
					g.count,
					CASE
						WHEN starts_with(g.merge_key, 'u:') THEN g.merge_key
						WHEN g.count > {lock_occ} THEN concat('u:', cast(g.tile_id AS VARCHAR), '#', cast(g.gbif_id AS VARCHAR))
						ELSE concat('m:', cast(g.tile_id AS VARCHAR))
					END AS merge_key
				FROM grouped g
				JOIN anchor a
				ON g.gbif_id = a.gbif_id AND g.tile_id = a.tile_id AND g.merge_key = a.merge_key
				WHERE a.rn = 1;
			""")
		# Append this propagated level into accumulation table
		timed_execute(db, f'{variant} level {level + 1} append', """
			INSERT INTO accum
			SELECT gbif_id, tile_id, center_lng, center_lat, count, merge_key
			FROM next_frontier;
		""")
		# Move next frontier into frontier for next iteration
		timed_execute(db, f'{variant} level {level + 1} advance', """
			CREATE OR REPLACE TEMP TABLE frontier AS
			SELECT gbif_id, tile_id, center_lng, center_lat, count, merge_key
			FROM next_frontier;
		""")
		# Increment recursion level counter
		level += 1
		# Print progress line
		print(f'ABTEST12 : {variant} propagated level {level} rows={row_count:,}')

# Finalize one variant into full habitat table for all taxa.
def finalize_variant(db: duckdb.DuckDBPyConnection, variant: str):
	# Keep low-rank source rows unchanged and replace high-rank rows with recomputed accum rows
	if variant == 'baseline':
		# Baseline high-rank rows merge by tile id
		timed_execute(db, f'{variant} finalize', f"""
			CREATE TEMP TABLE out_{variant} AS
			WITH low_rows AS (
				SELECT h.gbif_id, h.tile_id, h.center_lng, h.center_lat, h.count
				FROM read_parquet(?) h
				JOIN low_rank_taxa l ON h.gbif_id = l.gbif_id
			),
			high_rows AS (
				SELECT gbif_id, tile_id,
					sum(center_lng * count) / sum(count) AS center_lng,
					sum(center_lat * count) / sum(count) AS center_lat,
					sum(count) AS count
				FROM accum
				GROUP BY gbif_id, tile_id
			)
			SELECT * FROM low_rows
			UNION ALL
			SELECT * FROM high_rows;
		""", [CURRENT_ARGS['habitat']])
	# Guarded high-rank rows preserve separate merge keys once marked unique
	elif variant == 'guarded_anchor':
		# Guarded final rows group by merge key to prevent re-collapse of protected rows
		timed_execute(db, f'{variant} finalize', f"""
			CREATE TEMP TABLE out_{variant} AS
			WITH low_rows AS (
				SELECT h.gbif_id, h.tile_id, h.center_lng, h.center_lat, h.count
				FROM read_parquet(?) h
				JOIN low_rank_taxa l ON h.gbif_id = l.gbif_id
			),
			high_rows AS (
				SELECT gbif_id, tile_id,
					sum(center_lng * count) / sum(count) AS center_lng,
					sum(center_lat * count) / sum(count) AS center_lat,
					sum(count) AS count
				FROM accum
				GROUP BY gbif_id, tile_id, merge_key
			)
			SELECT * FROM low_rows
			UNION ALL
			SELECT * FROM high_rows;
		""", [CURRENT_ARGS['habitat']])
	# Abort unsupported variant
	else:
		# Raise explicit value error
		raise ValueError(f'Unsupported variant: {variant}')

# Write one finalized variant output to parquet.
def write_variant_output(db: duckdb.DuckDBPyConnection, variant: str, out_dir: str) -> str:
	# Resolve output path for current variant
	out_path = os.path.join(out_dir, f'abtest12_full_{variant}.parquet').replace('\\', '/')
	# Ensure output directory exists
	os.makedirs(out_dir, exist_ok=True)
	# Write variant table to parquet
	timed_execute(db, f'write {variant}', f"""
		COPY (SELECT gbif_id, tile_id, center_lng, center_lat, count FROM out_{variant})
		TO '{out_path}' (FORMAT PARQUET, COMPRESSION ZSTD);
	""")
	# Return output path
	return out_path

# Collect global metrics and one parent-specific metric from a variant table.
def collect_metrics(db: duckdb.DuckDBPyConnection, variant: str, parent_gbif: int) -> dict:
	# Collect global row and mass metrics
	global_row = db.execute(f"""
		SELECT count(*) AS rows, coalesce(sum(count),0) AS total_occ, coalesce(max(count),0) AS max_occ
		FROM out_{variant};
	""").fetchone()
	# Collect parent-specific metrics for requested parent taxon
	parent_row = db.execute(f"""
		SELECT count(*) AS rows, coalesce(sum(count),0) AS total_occ, coalesce(max(count),0) AS max_occ
		FROM out_{variant}
		WHERE gbif_id = ?;
	""", [parent_gbif]).fetchone()
	# Return metrics dictionary
	return {
		'global': {
			'rows': int(global_row[0]),
			'total_occ': int(global_row[1]),
			'max_occ': int(global_row[2]),
		},
		'parent': {
			'rows': int(parent_row[0]),
			'total_occ': int(parent_row[1]),
			'max_occ': int(parent_row[2]),
		},
	}

# Hold parsed args globally for finalize helper input binding.
CURRENT_ARGS = {}

# Main runner.
def main():
	# Build cli parser
	parser = argparse.ArgumentParser()
	# Select habitat parquet input path
	parser.add_argument('--habitat', default='data/temp/geo_abtest_habitat_20260401-7e70f9623fe7.parquet')
	# Select release parquet path
	parser.add_argument('--release-parquet', default='')
	# Select lock threshold for guarded variant
	parser.add_argument('--lock-occ', type=int, default=10000)
	# Select seed ranks for recomputation start (comma-separated, e.g. GENUS or SPECIES,GENUS)
	parser.add_argument('--seed-ranks', default='GENUS')
	# Select seed center mode: tile (current) or observed (bbox center from occurrences in tile)
	parser.add_argument('--seed-center-mode', default='tile', choices=['tile', 'observed'])
	# Select parent name for focused metrics
	parser.add_argument('--focus-parent', default='tracheophyta')
	# Limit seeds to focus subtree only for quicker targeted tests
	parser.add_argument('--focus-only', action='store_true')
	# Select rolling occurrences parquet used by observed seed mode
	parser.add_argument('--occurrences', default='data/geo/occurrences.parquet')
	# Select output directory for parquet and summary writes
	parser.add_argument('--out-dir', default='data/temp')
	# Parse cli args
	args = parser.parse_args()
	# Expose args for finalize helper
	CURRENT_ARGS.update({'habitat': args.habitat})
	# Validate habitat input exists
	if not os.path.isfile(args.habitat): raise FileNotFoundError(f'Missing habitat parquet: {args.habitat}')
	# Validate occurrences parquet when observed seed mode is requested
	if args.seed_center_mode == 'observed' and not os.path.isfile(args.occurrences):
		raise FileNotFoundError(f'Missing occurrences parquet for observed seed mode: {args.occurrences}')
	# Resolve release parquet input
	release_parquet = resolve_release_parquet(args.release_parquet or None)
	# Open duckdb connection
	db = open_db(args.out_dir)
	# Parse and normalize seed rank list
	seed_ranks = [rank.strip().upper() for rank in args.seed_ranks.split(',') if rank.strip()]
	# Abort when no seed ranks are provided
	if not seed_ranks: raise RuntimeError('No seed ranks provided via --seed-ranks')
	# Prepare taxonomy and parent edge tables
	prepare_taxonomy(db, release_parquet, seed_ranks)
	# Resolve focus parent gbif id from loaded taxonomy table
	focus_parent_gbif = db.execute("""
		SELECT gbif_id
		FROM taxa
		WHERE name_consensus = lower(?)
		LIMIT 1;
	""", [args.focus_parent]).fetchone()
	# Abort if focus parent missing
	if not focus_parent_gbif: raise RuntimeError(f'Focus parent not found: {args.focus_parent}')
	# Keep focus parent integer
	focus_parent_gbif = int(focus_parent_gbif[0])
	# Optionally scope seeds to focus parent subtree for targeted runs
	if args.focus_only:
		# Build focus subtree taxon ids from accepted taxonomy graph
		timed_execute(db, 'build focus subtree', """
			CREATE TEMP TABLE focus_subtree AS
			WITH RECURSIVE subtree(gbif_id, id_meso) AS (
				SELECT gbif_id, id_meso FROM taxa WHERE gbif_id = ?
				UNION ALL
				SELECT t.gbif_id, t.id_meso
				FROM taxa t
				JOIN subtree s ON t.parent_consensus = s.id_meso
			)
			SELECT gbif_id FROM subtree;
		""", [focus_parent_gbif])
		# Restrict seed taxon set to requested ranks inside focus subtree
		timed_execute(db, 'scope seed ranks to focus subtree', """
			CREATE OR REPLACE TEMP TABLE low_rank_taxa AS
			SELECT l.gbif_id
			FROM low_rank_taxa l
			JOIN focus_subtree f ON l.gbif_id = f.gbif_id;
		""")
	# Prepare seed habitat rows from selected seed mode
	prepare_seed_habitat(db, args.habitat, args.occurrences, args.seed_center_mode)

	# Run baseline variant end-to-end
	build_initial_frontier(db, 'baseline', args.lock_occ)
	run_recursive_rollup(db, 'baseline', args.lock_occ)
	finalize_variant(db, 'baseline')
	baseline_path = write_variant_output(db, 'baseline', args.out_dir)
	baseline_metrics = collect_metrics(db, 'baseline', focus_parent_gbif)

	# Reset frontier and accum tables for guarded variant
	timed_execute(db, 'reset variant tables', """
		DROP TABLE IF EXISTS frontier;
		DROP TABLE IF EXISTS accum;
		DROP TABLE IF EXISTS joined;
		DROP TABLE IF EXISTS next_frontier;
	""")

	# Run guarded_anchor variant end-to-end
	build_initial_frontier(db, 'guarded_anchor', args.lock_occ)
	run_recursive_rollup(db, 'guarded_anchor', args.lock_occ)
	finalize_variant(db, 'guarded_anchor')
	guarded_path = write_variant_output(db, 'guarded_anchor', args.out_dir)
	guarded_metrics = collect_metrics(db, 'guarded_anchor', focus_parent_gbif)

	# Build reference metrics from input habitat parquet
	ref_global = db.execute('SELECT count(*), coalesce(sum(count),0), coalesce(max(count),0) FROM read_parquet(?)', [args.habitat]).fetchone()
	ref_parent = db.execute('SELECT count(*), coalesce(sum(count),0), coalesce(max(count),0) FROM read_parquet(?) WHERE gbif_id = ?', [args.habitat, focus_parent_gbif]).fetchone()

	# Build summary payload
	summary = {
		'inputs': {
			'habitat': args.habitat,
			'release_parquet': release_parquet,
			'focus_parent': args.focus_parent,
			'focus_parent_gbif': focus_parent_gbif,
			'lock_occ': args.lock_occ,
			'seed_ranks': seed_ranks,
			'seed_center_mode': args.seed_center_mode,
			'focus_only': bool(args.focus_only),
			'occurrences': args.occurrences,
		},
		'reference': {
			'global': {'rows': int(ref_global[0]), 'total_occ': int(ref_global[1]), 'max_occ': int(ref_global[2])},
			'parent': {'rows': int(ref_parent[0]), 'total_occ': int(ref_parent[1]), 'max_occ': int(ref_parent[2])},
		},
		'baseline': baseline_metrics,
		'guarded_anchor': guarded_metrics,
		'outputs': {
			'baseline_parquet': baseline_path,
			'guarded_anchor_parquet': guarded_path,
		},
	}
	# Resolve summary output path
	summary_path = os.path.join(args.out_dir, 'abtest12_full_summary.json')
	# Persist summary json
	with open(summary_path, 'w', encoding='utf-8') as file:
		json.dump(summary, file, indent=2)
	# Print summary for operator
	print(json.dumps(summary, indent=2))
	# Print summary path
	print(f'ABTEST12 : wrote summary {summary_path}')

# Execute main entrypoint.
if __name__ == '__main__':
	main()
