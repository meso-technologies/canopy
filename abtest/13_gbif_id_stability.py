# Compare candidate GBIF ID stabilization strategies against local hydrated canopy data.
from types import SimpleNamespace
# Load timing helpers for basic runtime comparisons.
import time
# Load DuckDB for set-based A/B probes.
import duckdb
# Load canopy runtime paths and settings proxy.
from importer.canopy import TMP_DIR, settings, build_settings
# Load latest processed-file discovery.
from importer.canopy.utils.filehandlers import get_latest_processed
# Load fuse helpers so the probe uses the same initial backbone construction as production.
from importer.canopy.pipeline import fuse as fuse_pipeline

# Candidate GBIF names observed in release churn examples.
SENTINELS = [
	# Parent/canonical churn example from the handover.
	'erica',
	# Accepted flip example from the handover.
	'cortinarius udolivascens lilacinostipitatus',
]

# Print a compact section header for readable console output.
def section(title):
	# Separate sections visually in long runs.
	print(f"\n############### {title} ###############", flush=True)

# Format seconds with one decimal place.
def elapsed(start):
	# Return wall-clock duration for a measured step.
	return f"{time.time() - start:.1f}s"

# Initialize canopy settings for local hydrated data access.
def init_runtime():
	# Build a minimal args object matching canopy CLI settings fields.
	args = SimpleNamespace(debug=False, verbose=False, force=False, csv=False, s3=False)
	# Install runtime settings into the canopy settings proxy.
	settings.set_config(build_settings(args))

# Load processed source parquets and build the same pre-add_ids backbone used by fuse.
def build_initial_meso(db):
	# Resolve latest local processed parquet inventory.
	results = get_latest_processed()
	# Load map-phase source tables through production helper.
	fuse_pipeline.load_map_sources(results, db)
	# Build initial meso rows through production helper.
	fuse_pipeline.initial_backbone(results, db)
	# Return source inventory for possible diagnostics.
	return results

# Apply the add_ids prelude up to, but not including, the per-authority name fallback.
def apply_add_ids_prelude(db):
	# Keep authority list aligned with production add_ids.
	id_authorities = fuse_pipeline.core_authorities + ['ncbi']
	# Ensure Wikidata target column exists before ID supplementation.
	db.execute("ALTER TABLE meso ADD COLUMN IF NOT EXISTS wikidata_id VARCHAR;")
	# Ensure all authority ID columns exist and seed Wikidata IDs from already-linked authority IDs.
	for source in id_authorities:
		# Build column name used by meso and Wikidata.
		id_col = f"{source}_id"
		# Create missing ID column using production integer-ID convention.
		db.execute(f"ALTER TABLE meso ADD COLUMN IF NOT EXISTS {id_col} {'UINTEGER' if id_col in fuse_pipeline.int_ids else 'VARCHAR'};")
		# Skip sources that production skips for this prelude.
		if source in ['wcvp', 'wikidata']: continue
		# Attach Wikidata IDs from already-known source IDs.
		db.execute(f"UPDATE meso m SET wikidata_id = w.id_raw FROM wikidata w WHERE wikidata_id IS NULL AND w.{id_col} IS NOT NULL AND m.{id_col} = w.{id_col};")
	# Fill remaining Wikidata IDs by exact name, matching production behavior.
	db.execute("""
		UPDATE meso m SET wikidata_id = w.id_raw FROM wikidata w
		WHERE wikidata_id IS NULL AND m.name_clean = w.name_clean AND NOT EXISTS (SELECT 1 FROM meso WHERE wikidata_id = w.id_raw);
	""")
	# Supplement all known source IDs from Wikidata before testing GBIF name fallback.
	db.execute(f"""
		UPDATE meso m SET {','.join([f'{authority}_id = COALESCE(m.{authority}_id, w.{authority}_id{('::UINTEGER' if str(authority + '_id') in fuse_pipeline.int_ids else '')})' for authority in id_authorities if authority not in ('wikidata', 'wcvp')])}
		FROM wikidata w WHERE m.wikidata_id = w.id_raw;
	""")

# Materialize a target table for one add_ids fallback option.
def run_addids_option(db, option, sql):
	# Start timing this option.
	start = time.time()
	# Recreate the target table from the shared pre-fallback snapshot.
	db.execute(f"CREATE OR REPLACE TEMP TABLE meso_{option} AS SELECT * FROM meso_prefallback;")
	# Execute the candidate exact-name fallback update.
	db.execute(sql.format(table=f"meso_{option}"))
	# Count GBIF IDs populated by the option.
	count = db.execute(f"SELECT COUNT(gbif_id) FROM meso_{option};").fetchone()[0]
	# Report option runtime and resulting coverage.
	print(f"add_ids option {option}: gbif_populated={count:,} runtime={elapsed(start)}", flush=True)

# Compare three add_ids fallback variants.
def compare_addids_options(db):
	# Announce this probe phase.
	section('add_ids gbif exact-name fallback')
	# Preserve the shared pre-fallback meso state.
	db.execute("CREATE OR REPLACE TEMP TABLE meso_prefallback AS SELECT * FROM meso;")
	# Count rows still eligible for GBIF exact-name fallback.
	eligible = db.execute("SELECT COUNT(*) FROM meso_prefallback WHERE gbif_id IS NULL;").fetchone()[0]
	# Count names with more than one GBIF candidate status.
	ambiguous_names = db.execute("""
		SELECT COUNT(*) FROM (
			SELECT name_clean FROM gbif GROUP BY name_clean HAVING COUNT(*) > 1 AND COUNT(DISTINCT status_clean) > 1
		);
	""").fetchone()[0]
	# Count potential target names that have mixed GBIF accepted/synonym candidates.
	eligible_ambiguous = db.execute("""
		SELECT COUNT(*) FROM (
			SELECT m.name_clean
			FROM (SELECT DISTINCT name_clean FROM meso_prefallback WHERE gbif_id IS NULL) m
			JOIN gbif g USING (name_clean)
			GROUP BY m.name_clean HAVING COUNT(*) > 1 AND COUNT(DISTINCT g.status_clean) > 1
		);
	""").fetchone()[0]
	# Print baseline ambiguity scale.
	print(f"eligible_meso_rows={eligible:,} gbif_mixed_status_names={ambiguous_names:,} eligible_mixed_status_names={eligible_ambiguous:,}", flush=True)
	# Production-current option leaves target and source selection under-specified.
	current_sql = """
		WITH single_matches AS (
			SELECT m.rowid AS meso_rowid, a.id_raw AS auth_id
			FROM gbif a
			JOIN (
				SELECT DISTINCT ON (name_clean) rowid, name_clean FROM {table} WHERE gbif_id IS NULL
			) m ON m.name_clean = a.name_clean
			WHERE NOT EXISTS (SELECT 1 FROM {table} WHERE gbif_id = a.id_raw)
		)
		UPDATE {table} SET gbif_id = sm.auth_id
		FROM single_matches sm WHERE {table}.rowid = sm.meso_rowid;
	"""
	# Deterministic generic option orders source candidates by ID only.
	generic_sql = """
		WITH single_matches AS (
			SELECT DISTINCT ON (m.rowid) m.rowid AS meso_rowid, a.id_raw AS auth_id
			FROM gbif a
			JOIN (
				SELECT DISTINCT ON (name_clean) rowid, name_clean FROM {table} WHERE gbif_id IS NULL ORDER BY name_clean, rowid
			) m ON m.name_clean = a.name_clean
			WHERE NOT EXISTS (SELECT 1 FROM {table} WHERE gbif_id = a.id_raw)
			ORDER BY m.rowid, a.id_raw::VARCHAR
		)
		UPDATE {table} SET gbif_id = sm.auth_id
		FROM single_matches sm WHERE {table}.rowid = sm.meso_rowid;
	"""
	# GBIF-aware option prefers accepted candidates before synonyms and other statuses.
	accepted_sql = """
		WITH single_matches AS (
			SELECT DISTINCT ON (m.rowid) m.rowid AS meso_rowid, a.id_raw AS auth_id
			FROM gbif a
			JOIN (
				SELECT DISTINCT ON (name_clean) rowid, name_clean FROM {table} WHERE gbif_id IS NULL ORDER BY name_clean, rowid
			) m ON m.name_clean = a.name_clean
			WHERE NOT EXISTS (SELECT 1 FROM {table} WHERE gbif_id = a.id_raw)
			ORDER BY m.rowid,
				CASE WHEN a.status_clean = 'accepted' THEN 0 WHEN a.status_clean = 'synonym' THEN 1 ELSE 2 END,
				a.id_raw::VARCHAR
		)
		UPDATE {table} SET gbif_id = sm.auth_id
		FROM single_matches sm WHERE {table}.rowid = sm.meso_rowid;
	"""
	# Run the current production shape.
	run_addids_option(db, 'current', current_sql)
	# Run deterministic ID-only ordering.
	run_addids_option(db, 'generic', generic_sql)
	# Run deterministic GBIF status-aware ordering.
	run_addids_option(db, 'accepted', accepted_sql)
	# Compare option outputs pairwise against status of selected GBIF IDs.
	db.execute("""
		CREATE OR REPLACE TEMP TABLE addids_compare AS
		SELECT
			c.rowid AS rowid,
			c.name_clean,
			c.gbif_id AS current_id,
			g.gbif_id AS generic_id,
			a.gbif_id AS accepted_id,
			gc.status_clean AS current_status,
			gg.status_clean AS generic_status,
			ga.status_clean AS accepted_status
		FROM meso_current c
		JOIN meso_generic g ON c.rowid = g.rowid
		JOIN meso_accepted a ON c.rowid = a.rowid
		LEFT JOIN gbif gc ON c.gbif_id = gc.id_raw
		LEFT JOIN gbif gg ON g.gbif_id = gg.id_raw
		LEFT JOIN gbif ga ON a.gbif_id = ga.id_raw;
	""")
	# Print high-level difference counts.
	db.sql("""
		SELECT
			COUNT(*) AS rows,
			SUM((current_id IS DISTINCT FROM generic_id)::INT) AS current_vs_generic_id_diff,
			SUM((current_id IS DISTINCT FROM accepted_id)::INT) AS current_vs_accepted_id_diff,
			SUM((generic_id IS DISTINCT FROM accepted_id)::INT) AS generic_vs_accepted_id_diff,
			SUM((current_status != 'accepted' AND accepted_status = 'accepted')::INT) AS current_nonaccepted_to_accepted,
			SUM((generic_status != 'accepted' AND accepted_status = 'accepted')::INT) AS generic_nonaccepted_to_accepted
		FROM addids_compare;
	""").show()
	# Print sentinel rows to verify concrete examples.
	db.sql(f"""
		SELECT name_clean, current_id, current_status, generic_id, generic_status, accepted_id, accepted_status
		FROM addids_compare
		WHERE name_clean IN ({','.join([repr(s) for s in SENTINELS])})
		ORDER BY name_clean;
	""").show(max_rows=20)
	# Print examples where the accepted-aware option changes a synonym/problematic current choice.
	db.sql("""
		SELECT name_clean, current_id, current_status, accepted_id, accepted_status
		FROM addids_compare
		WHERE current_id IS DISTINCT FROM accepted_id AND current_status != 'accepted' AND accepted_status = 'accepted'
		ORDER BY name_clean
		LIMIT 20;
	""").show(max_rows=20)

# Prepare a release-like accepted table for polish query-shape tests.
def prepare_polish_tables(db):
	# Announce this probe phase.
	section('polish gbif fallback query options')
	# Build a compact accepted meso slice from the latest local release parquet.
	db.execute("""
		CREATE OR REPLACE TEMP TABLE meso_polish_source AS
		SELECT row_number() OVER () AS probe_rowid, name_consensus, gbif_id AS original_gbif_id, accepted
		FROM read_parquet('importer/canopy/data/releases/20260506-e738733531be/20260506-e738733531be.parquet')
		WHERE accepted;
	""")
	# Null GBIF IDs intentionally so fallback options can be compared on all accepted names.
	db.execute("""
		CREATE OR REPLACE TEMP TABLE meso_polish_base AS
		SELECT probe_rowid, name_consensus, CAST(NULL AS UINTEGER) AS gbif_id, original_gbif_id, accepted
		FROM meso_polish_source;
	""")
	# Report baseline accepted rows and original missing GBIF IDs.
	db.sql("""
		SELECT COUNT(*) AS accepted_rows, SUM((original_gbif_id IS NULL)::INT) AS original_missing_gbif_ids
		FROM meso_polish_base;
	""").show()

# Compare three ways to build accepted-name lookup for outdated GBIF IDs.
def compare_polish_accepted_lookup(db):
	# Start section timing.
	section('polish step 1 accepted-name lookup')
	# Option 1 mirrors current MODE aggregation.
	start = time.time()
	db.execute("CREATE OR REPLACE TEMP TABLE accepted_mode AS SELECT name_clean, MODE(id_raw) AS accepted_id FROM gbif WHERE status_clean = 'accepted' GROUP BY name_clean;")
	print(f"lookup option mode: rows={db.execute('SELECT COUNT(*) FROM accepted_mode').fetchone()[0]:,} runtime={elapsed(start)}", flush=True)
	# Option 2 uses deterministic ordered FIRST aggregation.
	start = time.time()
	db.execute("CREATE OR REPLACE TEMP TABLE accepted_first AS SELECT name_clean, FIRST(id_raw ORDER BY id_raw) AS accepted_id FROM gbif WHERE status_clean = 'accepted' GROUP BY name_clean;")
	print(f"lookup option first_ordered: rows={db.execute('SELECT COUNT(*) FROM accepted_first').fetchone()[0]:,} runtime={elapsed(start)}", flush=True)
	# Option 3 uses a window rank to test an equivalent deterministic query shape.
	start = time.time()
	db.execute("""
		CREATE OR REPLACE TEMP TABLE accepted_window AS
		SELECT name_clean, id_raw AS accepted_id
		FROM (
			SELECT name_clean, id_raw, row_number() OVER (PARTITION BY name_clean ORDER BY id_raw) AS rn
			FROM gbif WHERE status_clean = 'accepted'
		) WHERE rn = 1;
	""")
	print(f"lookup option window_rank: rows={db.execute('SELECT COUNT(*) FROM accepted_window').fetchone()[0]:,} runtime={elapsed(start)}", flush=True)
	# Compare whether deterministic alternatives differ from MODE.
	db.sql("""
		SELECT
			COUNT(*) AS lookup_names,
			SUM((m.accepted_id IS DISTINCT FROM f.accepted_id)::INT) AS mode_vs_first_id_diff,
			SUM((f.accepted_id IS DISTINCT FROM w.accepted_id)::INT) AS first_vs_window_id_diff
		FROM accepted_mode m
		JOIN accepted_first f USING (name_clean)
		JOIN accepted_window w USING (name_clean);
	""").show()

# Compare three accepted-ID fill strategies for accepted meso rows with NULL GBIF ID.
def compare_polish_accepted_fill(db):
	# Announce accepted fill phase.
	section('polish step 2 accepted-only null fill')
	# Define the current scalar LIMIT shape.
	current_sql = """
		UPDATE {table} SET gbif_id = (
			SELECT s.id_raw FROM gbif s
			WHERE s.name_clean = {table}.name_consensus AND s.status_clean = 'accepted'
			LIMIT 1
		)
		WHERE gbif_id IS NULL;
	"""
	# Define deterministic scalar ORDER BY shape.
	ordered_sql = """
		UPDATE {table} SET gbif_id = (
			SELECT s.id_raw FROM gbif s
			WHERE s.name_clean = {table}.name_consensus AND s.status_clean = 'accepted'
			ORDER BY s.id_raw
			LIMIT 1
		)
		WHERE gbif_id IS NULL;
	"""
	# Define precomputed lookup join shape.
	join_sql = """
		UPDATE {table} SET gbif_id = l.accepted_id
		FROM accepted_first l
		WHERE {table}.gbif_id IS NULL AND {table}.name_consensus = l.name_clean;
	"""
	# Run three update variants against identical copies.
	for option, sql in [('limit', current_sql), ('ordered', ordered_sql), ('lookup_join', join_sql)]:
		# Start from the same accepted release slice.
		db.execute(f"CREATE OR REPLACE TEMP TABLE fill_{option} AS SELECT * FROM meso_polish_base;")
		# Time the update query.
		start = time.time()
		# Execute the candidate update.
		db.execute(sql.format(table=f"fill_{option}"))
		# Count remaining nulls after update.
		missing = db.execute(f"SELECT SUM((gbif_id IS NULL)::INT) FROM fill_{option};").fetchone()[0]
		# Print result for this option.
		print(f"accepted fill option {option}: missing_after={missing:,} runtime={elapsed(start)}", flush=True)
	# Compare chosen IDs from all three options.
	db.sql("""
		SELECT
			COUNT(*) AS rows,
			SUM((l.gbif_id IS DISTINCT FROM o.gbif_id)::INT) AS limit_vs_ordered_diff,
			SUM((o.gbif_id IS DISTINCT FROM j.gbif_id)::INT) AS ordered_vs_join_diff
		FROM fill_limit l
		JOIN fill_ordered o USING (probe_rowid)
		JOIN fill_lookup_join j USING (probe_rowid);
	""").show()

# Compare three generic GBIF fill strategies for accepted meso rows still NULL after accepted-only fill.
def compare_polish_generic_fill(db):
	# Announce generic fill phase.
	section('polish step 3 generic null fill')
	# Create a status-aware per-name lookup used by the join option.
	db.execute("""
		CREATE OR REPLACE TEMP TABLE gbif_name_preferred AS
		SELECT name_clean, id_raw AS preferred_id
		FROM (
			SELECT
				name_clean,
				id_raw,
				row_number() OVER (
					PARTITION BY name_clean
					ORDER BY CASE WHEN status_clean = 'accepted' THEN 0 WHEN status_clean = 'synonym' THEN 1 ELSE 2 END, id_raw
				) AS rn
			FROM gbif
		) WHERE rn = 1;
	""")
	# Define current scalar LIMIT shape.
	current_sql = """
		UPDATE {table} SET gbif_id = (
			SELECT s.id_raw FROM gbif s
			WHERE s.name_clean = {table}.name_consensus
			LIMIT 1
		)
		WHERE gbif_id IS NULL;
	"""
	# Define deterministic scalar status-aware ORDER BY shape.
	ordered_sql = """
		UPDATE {table} SET gbif_id = (
			SELECT s.id_raw FROM gbif s
			WHERE s.name_clean = {table}.name_consensus
			ORDER BY CASE WHEN s.status_clean = 'accepted' THEN 0 WHEN s.status_clean = 'synonym' THEN 1 ELSE 2 END, s.id_raw
			LIMIT 1
		)
		WHERE gbif_id IS NULL;
	"""
	# Define precomputed lookup join shape.
	join_sql = """
		UPDATE {table} SET gbif_id = l.preferred_id
		FROM gbif_name_preferred l
		WHERE {table}.gbif_id IS NULL AND {table}.name_consensus = l.name_clean;
	"""
	# Run three update variants after deterministic accepted-only fill.
	for option, sql in [('limit', current_sql), ('ordered', ordered_sql), ('lookup_join', join_sql)]:
		# Start from accepted-only fill output to isolate final generic fill behavior.
		db.execute(f"CREATE OR REPLACE TEMP TABLE generic_{option} AS SELECT * FROM fill_ordered;")
		# Time this generic update.
		start = time.time()
		# Execute the candidate update.
		db.execute(sql.format(table=f"generic_{option}"))
		# Count remaining nulls.
		missing = db.execute(f"SELECT SUM((gbif_id IS NULL)::INT) FROM generic_{option};").fetchone()[0]
		# Print result for this option.
		print(f"generic fill option {option}: missing_after={missing:,} runtime={elapsed(start)}", flush=True)
	# Compare generic fill IDs across options.
	db.sql("""
		SELECT
			COUNT(*) AS rows,
			SUM((l.gbif_id IS DISTINCT FROM o.gbif_id)::INT) AS limit_vs_ordered_diff,
			SUM((o.gbif_id IS DISTINCT FROM j.gbif_id)::INT) AS ordered_vs_join_diff
		FROM generic_limit l
		JOIN generic_ordered o USING (probe_rowid)
		JOIN generic_lookup_join j USING (probe_rowid);
	""").show()

# Run all probes.
def main():
	# Initialize local canopy runtime.
	init_runtime()
	# Connect to an in-memory DuckDB database.
	with duckdb.connect(':memory:') as db:
		# Route DuckDB spills to canopy temp directory.
		db.execute(f"SET temp_directory = '{TMP_DIR}'")
		# Build initial meso state from processed source parquets.
		section('building initial meso')
		# Measure initial build time.
		start = time.time()
		# Build source tables and initial backbone.
		build_initial_meso(db)
		# Apply production add_ids prelude before the exact-name fallback under test.
		apply_add_ids_prelude(db)
		# Print build timing and row count.
		print(f"initial_meso_rows={db.execute('SELECT COUNT(*) FROM meso').fetchone()[0]:,} runtime={elapsed(start)}", flush=True)
		# Compare add_ids GBIF name-match variants.
		compare_addids_options(db)
		# Prepare latest release slice for polish probes.
		prepare_polish_tables(db)
		# Compare accepted lookup variants.
		compare_polish_accepted_lookup(db)
		# Compare accepted-only NULL fill variants.
		compare_polish_accepted_fill(db)
		# Compare generic NULL fill variants.
		compare_polish_generic_fill(db)

# Execute script entrypoint.
if __name__ == '__main__':
	# Run the abtest.
	main()
