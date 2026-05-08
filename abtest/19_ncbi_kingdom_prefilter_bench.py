# Compare NCBI kingdom prefilter strategies and downstream ID retention.
import glob
import os
import time
import zipfile

import duckdb

# Keep the narrow target lineage roots explicit so the original proposal remains comparable.
ROOTS = (33090, 4751)
# Include Meso-relevant algae and fungi-like protist groups that NCBI does not place under Fungi or Viridiplantae.
EXPANDED_ROOTS = (
	33090,   # Viridiplantae
	4751,    # Fungi
	2763,    # Rhodophyta, red algae
	2696291, # Ochrophyta, brown algae / diatoms / golden algae
	2830,    # Haptophyta
	3027,    # Cryptophyceae
	2864,    # Dinophyceae, dinoflagellates
	38254,   # Glaucocystophyceae
	33682,   # Euglenozoa, includes euglenoid algae
	4762,    # Oomycota, fungi-like stramenopiles
	142796,  # Eumycetozoa, slime molds
	2779609, # Phytomyxea, plasmodiophorid plant parasites
	1117,    # Cyanobacteriota, blue-green algae
	419944,  # Picozoa, pico-algal/protist edge cases
)
# Trim the row-ending marker NCBI leaves on the final BCP column.
TRAIL = "chr(9) || '|'"
# Resolve paths relative to this abtest file.
BASE_DIR = os.path.abspath(os.path.join(os.path.dirname(__file__), '..'))
# Use the newest local NCBI source zip.
SOURCE = sorted(glob.glob(os.path.join(BASE_DIR, 'data', 'source', 'ncbi.*.zip')))[-1]
# Use the newest local staging release produced by canopy/www.
STAGING = sorted(glob.glob(os.path.abspath(os.path.join(BASE_DIR, '..', '..', 'data', 'importer', 'staging', '*'))))[-1]
# Use the postgres parquet because it contains served rows and ncbi_id.
POSTGRES = glob.glob(os.path.join(STAGING, 'postgres.*.parquet'))[0]
# Route DuckDB spill out of the repo root.
TMP = os.path.join(BASE_DIR, 'tmp')

# Read the NCBI DMP files with fixed types so DuckDB does not infer from messy text.
DMP_OPTS = dict(delimiter='\t|\t', header=False, quotechar='', parallel=True)
# Keep only node columns used by the filter/core build.
NODE_COLS = {
	'taxid':'UINTEGER', 'parent':'UINTEGER', 'rank':'VARCHAR', 'embl':'VARCHAR',
	'division_id':'UTINYINT', 'c5':'VARCHAR', 'c6':'VARCHAR', 'c7':'VARCHAR',
	'c8':'VARCHAR', 'c9':'VARCHAR', 'c10':'VARCHAR', 'c11':'VARCHAR',
	'c12':'VARCHAR', 'c13':'VARCHAR', 'c14':'VARCHAR', 'c15':'VARCHAR',
	'c16':'VARCHAR', 'c17':'VARCHAR',
}
# Read names for scientific names and authority strings.
NAME_COLS = {'taxid':'UINTEGER','name':'VARCHAR','unique_name':'VARCHAR','name_class':'VARCHAR'}
# Read taxid lineage for an alternative precomputed-lineage filter.
TAXIDLINEAGE_COLS = {'taxid':'UINTEGER','lineage_raw':'VARCHAR'}
# Read ranked lineage for an alternative kingdom-name filter and diagnostics.
RANKED_COLS = {
	'taxid':'UINTEGER','name':'VARCHAR','species':'VARCHAR','genus':'VARCHAR','family':'VARCHAR',
	'order_name':'VARCHAR','class_name':'VARCHAR','phylum':'VARCHAR','kingdom':'VARCHAR','superkingdom':'VARCHAR',
}

# Keep the current core query shape intact so this benchmark catches data-integrity changes.
CORE_SELECT = """
WITH sci AS (
	SELECT taxid, name AS sci_name FROM names WHERE name_class = 'scientific name'
), auth AS (
	SELECT n.taxid, any_value(n.name) AS authority_name
	FROM names n JOIN sci s ON n.taxid = s.taxid
	WHERE n.name_class = 'authority'
	AND starts_with(n.name, s.sci_name)
	AND length(n.name) > length(s.sci_name)
	GROUP BY n.taxid
)
SELECT
	n.taxid AS id_raw,
	s.sci_name AS name_raw,
	lower(s.sci_name) AS name_clean,
	n.rank AS rank_raw,
	CAST(NULL AS VARCHAR) AS rank_clean,
	n.parent AS parent_raw,
	NULLIF(trim(substring(a.authority_name, length(s.sci_name) + 1)), '') AS author_raw
FROM nodes n
JOIN eligible e ON e.taxid = n.taxid
JOIN sci s ON s.taxid = n.taxid
LEFT JOIN auth a ON a.taxid = n.taxid
"""

# Full core-build query using the expanded Meso scope.
EXPANDED_DOWN_RECURSIVE = f"""
	CREATE TEMP TABLE eligible AS
	WITH RECURSIVE scope(taxid) AS (
		SELECT taxid FROM nodes WHERE taxid IN {EXPANDED_ROOTS}
		UNION ALL
		SELECT n.taxid FROM nodes n JOIN scope s ON n.parent = s.taxid WHERE n.taxid <> n.parent
	)
	SELECT DISTINCT taxid FROM scope;
	CREATE TEMP TABLE result AS {CORE_SELECT};
"""

# Candidate prefilter strategies to compare.
APPROACHES = {
	'down_recursive_cte': f"""
		CREATE TEMP TABLE eligible AS
		WITH RECURSIVE scope(taxid) AS (
			SELECT taxid FROM nodes WHERE taxid IN {ROOTS}
			UNION ALL
			SELECT n.taxid FROM nodes n JOIN scope s ON n.parent = s.taxid WHERE n.taxid <> n.parent
		)
		SELECT DISTINCT taxid FROM scope;
		CREATE TEMP TABLE result AS {CORE_SELECT};
	""",
	'up_recursive_from_scientific': f"""
		CREATE TEMP TABLE eligible AS
		WITH RECURSIVE lineage(taxid, ancestor) AS (
			SELECT n.taxid, n.taxid AS ancestor
			FROM nodes n JOIN names s ON s.taxid = n.taxid AND s.name_class = 'scientific name'
			UNION ALL
			SELECT l.taxid, n.parent AS ancestor
			FROM lineage l JOIN nodes n ON n.taxid = l.ancestor
			WHERE l.ancestor <> n.parent
		)
		SELECT DISTINCT taxid FROM lineage WHERE ancestor IN {ROOTS};
		CREATE TEMP TABLE result AS {CORE_SELECT};
	""",
	'taxidlineage_exact_regex': f"""
		CREATE TEMP TABLE eligible AS
		SELECT taxid FROM taxidlineage
		WHERE taxid IN {ROOTS}
		OR regexp_matches(lineage, '(^| )33090( |$)')
		OR regexp_matches(lineage, '(^| )4751( |$)');
		CREATE TEMP TABLE result AS {CORE_SELECT};
	""",
	'rankedlineage_kingdom_name': f"""
		CREATE TEMP TABLE eligible AS
		SELECT taxid FROM rankedlineage
		WHERE taxid IN {ROOTS}
		OR kingdom IN ('Viridiplantae', 'Fungi');
		CREATE TEMP TABLE result AS {CORE_SELECT};
	""",
}

# Print a compact section marker.
def section(title):
	print(f"\n############### {title} ###############", flush=True)

# Run one approach and print stable integrity metrics.
def profile(db, name, sql):
	db.execute('DROP TABLE IF EXISTS eligible')
	db.execute('DROP TABLE IF EXISTS result')
	start = time.perf_counter()
	db.execute(sql)
	elapsed = time.perf_counter() - start
	metrics = db.execute("""
		SELECT
			count() AS row_count,
			count(author_raw) AS author_rows,
			sum(id_raw)::UBIGINT AS id_sum,
			bit_xor(hash(id_raw, name_raw, rank_raw, parent_raw, coalesce(author_raw, ''))) AS checksum
		FROM result
	""").fetchone()
	eligible = db.execute('SELECT count() FROM eligible').fetchone()[0]
	print(f"{name}\t{elapsed:.3f}s\teligible={eligible}\trows={metrics[0]}\tauth={metrics[1]}\tid_sum={metrics[2]}\tchecksum={metrics[3]}")
	return name, elapsed, eligible, metrics

# Compare all approaches against the exact recursive descendant reference.
def diff_approaches(db):
	db.execute('DROP TABLE IF EXISTS eligible')
	db.execute('DROP TABLE IF EXISTS result')
	db.execute(APPROACHES['down_recursive_cte'])
	db.execute('CREATE TEMP TABLE ref AS SELECT * FROM result')
	for name, sql in APPROACHES.items():
		db.execute('DROP TABLE IF EXISTS eligible')
		db.execute('DROP TABLE IF EXISTS result')
		db.execute(sql)
		diff = db.execute("""
			SELECT
				(SELECT count() FROM (SELECT id_raw FROM ref EXCEPT SELECT id_raw FROM result)) AS missing_from_candidate,
				(SELECT count() FROM (SELECT id_raw FROM result EXCEPT SELECT id_raw FROM ref)) AS extra_in_candidate,
				(SELECT count() FROM (SELECT * FROM ref EXCEPT SELECT * FROM result)) AS row_value_missing,
				(SELECT count() FROM (SELECT * FROM result EXCEPT SELECT * FROM ref)) AS row_value_extra
		""").fetchone()
		print(f"diff_vs_down_recursive\t{name}\tmissing={diff[0]}\textra={diff[1]}\trow_missing={diff[2]}\trow_extra={diff[3]}")

# Materialise the recursive descendant set for a root tuple and report its runtime.
def build_recursive_eligible(db, roots):
	db.execute('DROP TABLE IF EXISTS eligible')
	start = time.perf_counter()
	db.execute(f"""
		CREATE TEMP TABLE eligible AS
		WITH RECURSIVE scope(taxid) AS (
			SELECT taxid FROM nodes WHERE taxid IN {roots}
			UNION ALL
			SELECT n.taxid FROM nodes n JOIN scope s ON n.parent = s.taxid WHERE n.taxid <> n.parent
		)
		SELECT DISTINCT taxid FROM scope;
	""")
	elapsed = time.perf_counter() - start
	eligible = db.execute('SELECT count() FROM eligible').fetchone()[0]
	return elapsed, eligible

# Check whether current served ncbi_ids would disappear after a lineage prefilter.
def check_staging_missing_ids(db, label='narrow', roots=ROOTS):
	section(f'latest staging ncbi_id retention {label}')
	print(f"staging\t{STAGING}")
	print(f"postgres\t{POSTGRES}")
	elapsed, eligible_count = build_recursive_eligible(db, roots)
	print(f"eligible_build\t{elapsed:.3f}s")
	print(f"eligible_count\t{eligible_count}")
	db.execute('DROP TABLE IF EXISTS baseline_ids')
	db.execute('DROP TABLE IF EXISTS missing_ids')
	db.execute(f"""
		CREATE TEMP TABLE baseline_ids AS
		SELECT DISTINCT ncbi_id FROM read_parquet('{POSTGRES}') WHERE ncbi_id IS NOT NULL;
		CREATE TEMP TABLE missing_ids AS
		SELECT b.ncbi_id FROM baseline_ids b ANTI JOIN eligible e ON e.taxid = b.ncbi_id;
	""")
	counts = db.execute("""
		SELECT
			(SELECT count() FROM baseline_ids) AS baseline_distinct_ncbi_ids,
			(SELECT count() FROM baseline_ids b JOIN eligible e ON e.taxid = b.ncbi_id) AS retained_ids,
			(SELECT count() FROM missing_ids) AS missing_ids
	""").fetchone()
	print(f"baseline_distinct_ncbi_ids\t{counts[0]}")
	print(f"retained_ids\t{counts[1]}")
	print(f"missing_ids\t{counts[2]}")
	not_in_nodes = db.execute("SELECT count() FROM missing_ids m ANTI JOIN nodes n ON n.taxid = m.ncbi_id").fetchone()[0]
	print(f"missing_ids_not_in_nodes\t{not_in_nodes}")
	print('\nmissing_ids_by_ncbi_kingdom')
	print(db.execute("""
		SELECT coalesce(r.kingdom, '[null]') AS ncbi_kingdom, count() AS id_count
		FROM missing_ids m LEFT JOIN rankedlineage r ON r.taxid = m.ncbi_id
		GROUP BY ALL ORDER BY id_count DESC
	""").fetchdf().to_string(index=False))
	print('\nmissing_rows_by_meso_kingdom')
	print(db.execute(f"""
		WITH missing_rows AS (
			SELECT p.kingdom, p.ncbi_id
			FROM read_parquet('{POSTGRES}') p JOIN missing_ids m ON m.ncbi_id = p.ncbi_id
		)
		SELECT kingdom AS meso_kingdom, count() AS row_count, count(DISTINCT ncbi_id) AS id_count
		FROM missing_rows GROUP BY ALL ORDER BY row_count DESC
	""").fetchdf().to_string(index=False))
	print('\nmissing_sample')
	print(db.execute(f"""
		SELECT p.name, p.rank, p.kingdom AS meso_kingdom, p.ncbi_id, r.name AS ncbi_name, r.kingdom AS ncbi_kingdom, r.superkingdom AS ncbi_superkingdom
		FROM read_parquet('{POSTGRES}') p
		JOIN missing_ids m ON m.ncbi_id = p.ncbi_id
		LEFT JOIN rankedlineage r ON r.taxid = p.ncbi_id
		ORDER BY p.kingdom, p.name LIMIT 80
	""").fetchdf().to_string(index=False))
	print('\nmissing_names_with_in_scope_ncbi_homonym')
	print(db.execute(f"""
		WITH missing_names AS (
			SELECT DISTINCT p.name
			FROM read_parquet('{POSTGRES}') p JOIN missing_ids m ON m.ncbi_id = p.ncbi_id
		), homonyms AS (
			SELECT mn.name, n.taxid, r.kingdom, r.superkingdom
			FROM missing_names mn
			JOIN names n ON lower(n.name) = mn.name AND n.name_class = 'scientific name'
			JOIN eligible e ON e.taxid = n.taxid
			LEFT JOIN rankedlineage r ON r.taxid = n.taxid
		)
		SELECT count() AS homonym_rows, count(DISTINCT name) AS names_with_homonym, count(DISTINCT taxid) AS in_scope_ncbi_candidates
		FROM homonyms
	""").fetchdf().to_string(index=False))
	print('\nmissing_homonym_sample')
	print(db.execute(f"""
		WITH missing_names AS (
			SELECT DISTINCT p.name
			FROM read_parquet('{POSTGRES}') p JOIN missing_ids m ON m.ncbi_id = p.ncbi_id
		), homonyms AS (
			SELECT mn.name, n.taxid, r.kingdom, r.superkingdom
			FROM missing_names mn
			JOIN names n ON lower(n.name) = mn.name AND n.name_class = 'scientific name'
			JOIN eligible e ON e.taxid = n.taxid
			LEFT JOIN rankedlineage r ON r.taxid = n.taxid
		)
		SELECT * FROM homonyms ORDER BY name LIMIT 80
	""").fetchdf().to_string(index=False))

# Load input DMPs once, then run benchmarks and retention checks.
def main():
	os.makedirs(TMP, exist_ok=True)
	section('inputs')
	print(f"source\t{SOURCE}")
	with zipfile.ZipFile(SOURCE, 'r') as z, duckdb.connect(':memory:') as db:
		db.execute(f"SET temp_directory = '{TMP}'")
		db.register('nodes_raw', db.read_csv(z.open('nodes.dmp'), columns=NODE_COLS, **DMP_OPTS))
		db.register('names_raw', db.read_csv(z.open('names.dmp'), columns=NAME_COLS, **DMP_OPTS))
		db.register('taxidlineage_raw', db.read_csv(z.open('taxidlineage.dmp'), columns=TAXIDLINEAGE_COLS, **DMP_OPTS))
		db.register('rankedlineage_raw', db.read_csv(z.open('rankedlineage.dmp'), columns=RANKED_COLS, **DMP_OPTS))
		load_start = time.perf_counter()
		db.execute(f"""
			CREATE TEMP TABLE nodes AS SELECT taxid, parent, rank FROM nodes_raw;
			CREATE TEMP TABLE names AS SELECT taxid, name, rtrim(name_class, {TRAIL}) AS name_class FROM names_raw;
			CREATE TEMP TABLE taxidlineage AS SELECT taxid, rtrim(lineage_raw, {TRAIL}) AS lineage FROM taxidlineage_raw;
			CREATE TEMP TABLE rankedlineage AS
			SELECT taxid, name, rtrim(kingdom, {TRAIL}) AS kingdom, rtrim(superkingdom, {TRAIL}) AS superkingdom
			FROM rankedlineage_raw;
		""")
		print(f"load\t{time.perf_counter() - load_start:.3f}s")
		section('prefilter timing')
		for round_no in range(1, 4):
			print(f"round\t{round_no}")
			for name, sql in APPROACHES.items():
				profile(db, name, sql)
		section('expanded prefilter timing')
		for round_no in range(1, 4):
			print(f"round\t{round_no}")
			profile(db, 'expanded_down_recursive_cte', EXPANDED_DOWN_RECURSIVE)
		section('prefilter integrity')
		diff_approaches(db)
		check_staging_missing_ids(db, 'narrow', ROOTS)
		check_staging_missing_ids(db, 'expanded', EXPANDED_ROOTS)

if __name__ == '__main__':
	main()
