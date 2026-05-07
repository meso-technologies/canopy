# Compare candidate Wikidata ID stabilization strategies against local hydrated canopy data.
from types import SimpleNamespace
# Load timing helpers for option runtime comparisons.
import time
# Load DuckDB for set-based A/B probes.
import duckdb
# Load canopy runtime paths and settings proxy.
from importer.canopy import TMP_DIR, settings, build_settings
# Load latest processed-file discovery.
from importer.canopy.utils.filehandlers import get_latest_processed
# Load fuse helpers so the probe uses production backbone construction.
from importer.canopy.pipeline import fuse as fuse_pipeline

# Names that demonstrated run-to-run drift after GBIF fallback stabilization.
SENTINELS = [
	'sauroglossum nitidum',
	'ampelopsis heterophylla',
	'hebinanthe paniculata',
	'klenzea',
]

# Print a compact section header.
def section(title):
	# Separate output sections in the console.
	print(f"\n############### {title} ###############", flush=True)

# Format elapsed runtime for one option.
def elapsed(start):
	# Return seconds with one decimal place.
	return f"{time.time() - start:.1f}s"

# Initialize canopy settings for local hydrated files.
def init_runtime():
	# Build minimal CLI args for local, non-S3 analysis.
	args = SimpleNamespace(debug=False, verbose=False, force=False, csv=False, s3=False)
	# Install runtime settings into the canopy proxy.
	settings.set_config(build_settings(args))

# Build the same initial pre-add_ids meso table as production fuse.
def build_initial_meso(db):
	# Resolve latest processed source inventory.
	results = get_latest_processed()
	# Load source parquet tables through production helper.
	fuse_pipeline.load_map_sources(results, db)
	# Build initial meso backbone through production helper.
	fuse_pipeline.initial_backbone(results, db)
	# Return source inventory for completeness.
	return results

# Ensure all authority ID columns exist before Wikidata matching.
def ensure_id_columns(db):
	# Match production add_ids authority list.
	id_authorities = fuse_pipeline.core_authorities + ['ncbi']
	# Create the target Wikidata ID column.
	db.execute("ALTER TABLE meso ADD COLUMN IF NOT EXISTS wikidata_id VARCHAR;")
	# Ensure every authority ID column referenced by the loop exists.
	for source in id_authorities:
		# Build source-specific ID column name.
		id_col = f"{source}_id"
		# Use production integer-ID convention.
		db.execute(f"ALTER TABLE meso ADD COLUMN IF NOT EXISTS {id_col} {'UINTEGER' if id_col in fuse_pipeline.int_ids else 'VARCHAR'};")

# Apply one Wikidata authority-ID matching strategy to a table copy.
def run_authority_option(db, option, strategy):
	# Start from the same initial backbone for every option.
	db.execute(f"CREATE OR REPLACE TEMP TABLE meso_{option} AS SELECT * FROM meso_base;")
	# Match production add_ids authority list.
	id_authorities = fuse_pipeline.core_authorities + ['ncbi']
	# Start runtime timer.
	start = time.time()
	# Apply the per-authority Wikidata ID matching loop.
	for source in id_authorities:
		# Build source-specific ID column name.
		id_col = f"{source}_id"
		# Preserve production skips.
		if source in ['wcvp', 'wikidata']: continue
		# Current option mirrors production's under-specified UPDATE FROM.
		if strategy == 'current':
			db.execute(f"""
				UPDATE meso_{option} m SET wikidata_id = w.id_raw
				FROM wikidata w
				WHERE wikidata_id IS NULL AND w.{id_col} IS NOT NULL AND m.{id_col} = w.{id_col};
			""")
		# ID-only option picks one stable Wikidata row by QID.
		elif strategy == 'id_order':
			db.execute(f"""
				WITH wikidata_matches AS (
					SELECT DISTINCT ON (m.rowid) m.rowid AS meso_rowid, w.id_raw AS wikidata_id
					FROM meso_{option} m
					JOIN wikidata w ON w.{id_col} IS NOT NULL AND m.{id_col} = w.{id_col}
					WHERE m.wikidata_id IS NULL
					ORDER BY m.rowid, w.id_raw
				)
				UPDATE meso_{option} SET wikidata_id = wm.wikidata_id
				FROM wikidata_matches wm WHERE meso_{option}.rowid = wm.meso_rowid;
			""")
		# Name-aware option prefers Wikidata rows whose name matches the Meso row.
		elif strategy == 'name_aware':
			db.execute(f"""
				WITH wikidata_matches AS (
					SELECT DISTINCT ON (m.rowid) m.rowid AS meso_rowid, w.id_raw AS wikidata_id
					FROM meso_{option} m
					JOIN wikidata w ON w.{id_col} IS NOT NULL AND m.{id_col} = w.{id_col}
					WHERE m.wikidata_id IS NULL
					ORDER BY m.rowid,
						CASE WHEN w.name_clean = m.name_clean THEN 0 ELSE 1 END,
						COALESCE(w.page_count, 0) DESC,
						w.id_raw
				)
				UPDATE meso_{option} SET wikidata_id = wm.wikidata_id
				FROM wikidata_matches wm WHERE meso_{option}.rowid = wm.meso_rowid;
			""")
		# Abort on programming errors in option names.
		else: raise RuntimeError(f"Unknown strategy {strategy}")
	# Count assigned Wikidata IDs.
	assigned = db.execute(f"SELECT COUNT(wikidata_id) FROM meso_{option};").fetchone()[0]
	# Count name mismatches among assigned Wikidata IDs.
	mismatches = db.execute(f"""
		SELECT COUNT(*)
		FROM meso_{option} m JOIN wikidata w ON m.wikidata_id = w.id_raw
		WHERE m.name_clean IS DISTINCT FROM w.name_clean;
	""").fetchone()[0]
	# Print result line.
	print(f"authority option {option}: assigned={assigned:,} name_mismatches={mismatches:,} runtime={elapsed(start)}", flush=True)

# Apply one Wikidata exact-name fallback strategy after authority matching.
def run_name_fallback_option(db, option, strategy):
	# Start timer for this fallback option.
	start = time.time()
	# Current option mirrors production's under-specified name match.
	if strategy == 'current':
		db.execute(f"""
			UPDATE meso_{option} m SET wikidata_id = w.id_raw FROM wikidata w
			WHERE wikidata_id IS NULL AND m.name_clean = w.name_clean AND NOT EXISTS (SELECT 1 FROM meso_{option} WHERE wikidata_id = w.id_raw);
		""")
	# ID-order option makes name fallback deterministic by QID.
	elif strategy == 'id_order':
		db.execute(f"""
			WITH wikidata_name_matches AS (
				SELECT DISTINCT ON (m.rowid) m.rowid AS meso_rowid, w.id_raw AS wikidata_id
				FROM meso_{option} m
				JOIN wikidata w ON m.name_clean = w.name_clean
				WHERE m.wikidata_id IS NULL AND NOT EXISTS (SELECT 1 FROM meso_{option} used WHERE used.wikidata_id = w.id_raw)
				ORDER BY m.rowid, w.id_raw
			)
			UPDATE meso_{option} SET wikidata_id = wm.wikidata_id
			FROM wikidata_name_matches wm WHERE meso_{option}.rowid = wm.meso_rowid;
		""")
	# Pagecount option uses name equality plus richer/pagecount tie-break.
	elif strategy == 'pagecount':
		db.execute(f"""
			WITH wikidata_name_matches AS (
				SELECT DISTINCT ON (m.rowid) m.rowid AS meso_rowid, w.id_raw AS wikidata_id
				FROM meso_{option} m
				JOIN wikidata w ON m.name_clean = w.name_clean
				WHERE m.wikidata_id IS NULL AND NOT EXISTS (SELECT 1 FROM meso_{option} used WHERE used.wikidata_id = w.id_raw)
				ORDER BY m.rowid, COALESCE(w.page_count, 0) DESC, w.id_raw
			)
			UPDATE meso_{option} SET wikidata_id = wm.wikidata_id
			FROM wikidata_name_matches wm WHERE meso_{option}.rowid = wm.meso_rowid;
		""")
	# Abort on programming errors in option names.
	else: raise RuntimeError(f"Unknown name fallback strategy {strategy}")
	# Count final assignments.
	assigned = db.execute(f"SELECT COUNT(wikidata_id) FROM meso_{option};").fetchone()[0]
	# Print result line.
	print(f"name fallback option {option}: assigned={assigned:,} runtime={elapsed(start)}", flush=True)

# Apply production Wikidata source-ID supplementation to each option.
def supplement_from_wikidata(db, option):
	# Match production authority list.
	id_authorities = fuse_pipeline.core_authorities + ['ncbi']
	# Start timer for supplementation.
	start = time.time()
	# Copy IDs from the selected Wikidata row using production COALESCE shape.
	db.execute(f"""
		UPDATE meso_{option} m SET {','.join([f'{authority}_id = COALESCE(m.{authority}_id, w.{authority}_id{('::UINTEGER' if str(authority + '_id') in fuse_pipeline.int_ids else '')})' for authority in id_authorities if authority not in ('wikidata', 'wcvp')])}
		FROM wikidata w WHERE m.wikidata_id = w.id_raw;
	""")
	# Count GBIF and CoL IDs after supplementation because these drive acceptance drift examples.
	gbif_count = db.execute(f"SELECT COUNT(gbif_id) FROM meso_{option};").fetchone()[0]
	col_count = db.execute(f"SELECT COUNT(col_id) FROM meso_{option};").fetchone()[0]
	# Print result line.
	print(f"supplement option {option}: gbif_ids={gbif_count:,} col_ids={col_count:,} runtime={elapsed(start)}", flush=True)

# Compare final option tables.
def compare_options(db):
	# Announce comparison phase.
	section('option comparison')
	# Materialize a compact comparison table.
	db.execute("""
		CREATE OR REPLACE TEMP TABLE compare AS
		SELECT
			c.rowid,
			c.name_clean,
			c.wikidata_id AS current_wikidata,
			i.wikidata_id AS id_order_wikidata,
			n.wikidata_id AS name_aware_wikidata,
			wc.name_clean AS current_wikiname,
			wi.name_clean AS id_order_wikiname,
			wn.name_clean AS name_aware_wikiname,
			c.gbif_id AS current_gbif,
			i.gbif_id AS id_order_gbif,
			n.gbif_id AS name_aware_gbif,
			c.col_id AS current_col,
			i.col_id AS id_order_col,
			n.col_id AS name_aware_col,
			c.inaturalist_id AS current_inat,
			i.inaturalist_id AS id_order_inat,
			n.inaturalist_id AS name_aware_inat
		FROM meso_current c
		JOIN meso_id_order i ON c.rowid = i.rowid
		JOIN meso_name_aware n ON c.rowid = n.rowid
		LEFT JOIN wikidata wc ON c.wikidata_id = wc.id_raw
		LEFT JOIN wikidata wi ON i.wikidata_id = wi.id_raw
		LEFT JOIN wikidata wn ON n.wikidata_id = wn.id_raw;
	""")
	# Print high-level difference counts.
	db.sql("""
		SELECT
			COUNT(*) AS row_count,
			SUM((current_wikidata IS DISTINCT FROM id_order_wikidata)::INT) AS current_vs_id_order_wikidata_diff,
			SUM((current_wikidata IS DISTINCT FROM name_aware_wikidata)::INT) AS current_vs_name_aware_wikidata_diff,
			SUM((id_order_wikidata IS DISTINCT FROM name_aware_wikidata)::INT) AS id_order_vs_name_aware_wikidata_diff,
			SUM((current_gbif IS DISTINCT FROM name_aware_gbif)::INT) AS current_vs_name_aware_gbif_diff,
			SUM((current_col IS DISTINCT FROM name_aware_col)::INT) AS current_vs_name_aware_col_diff,
			SUM((current_inat IS DISTINCT FROM name_aware_inat)::INT) AS current_vs_name_aware_inat_diff
		FROM compare;
	""").show()
	# Print name mismatch counts by option.
	db.sql("""
		SELECT
			SUM((current_wikidata IS NOT NULL AND current_wikiname IS DISTINCT FROM name_clean)::INT) AS current_name_mismatches,
			SUM((id_order_wikidata IS NOT NULL AND id_order_wikiname IS DISTINCT FROM name_clean)::INT) AS id_order_name_mismatches,
			SUM((name_aware_wikidata IS NOT NULL AND name_aware_wikiname IS DISTINCT FROM name_clean)::INT) AS name_aware_name_mismatches
		FROM compare;
	""").show()
	# Print sentinel rows for direct spot-checking.
	db.sql(f"""
		SELECT name_clean, current_wikidata, current_wikiname, name_aware_wikidata, name_aware_wikiname, current_gbif, name_aware_gbif, current_col, name_aware_col, current_inat, name_aware_inat
		FROM compare
		WHERE name_clean IN ({','.join([repr(s) for s in SENTINELS])})
		ORDER BY name_clean;
	""").show(max_rows=30, max_width=220)
	# Print additional examples where current chooses a different-name Wikidata row but name-aware fixes it.
	db.sql("""
		SELECT name_clean, current_wikidata, current_wikiname, name_aware_wikidata, name_aware_wikiname, current_gbif, name_aware_gbif, current_col, name_aware_col, current_inat, name_aware_inat
		FROM compare
		WHERE current_wikidata IS DISTINCT FROM name_aware_wikidata
		AND current_wikiname IS DISTINCT FROM name_clean
		AND name_aware_wikiname = name_clean
		ORDER BY name_clean
		LIMIT 25;
	""").show(max_rows=25, max_width=220)

# Find examples from existing local run drift where Wikidata changed between run1 and run2.
def find_existing_drift_examples(db):
	# Announce drift inspection phase.
	section('existing run drift examples')
	# Load the two local deterministic fuse rebuilds if available.
	db.execute("""
		CREATE OR REPLACE TEMP TABLE drift_run1 AS
		SELECT id_meso, name_consensus, accepted, wikidata_id, gbif_id, col_id, inaturalist_id, accepted_by, considered_synonym
		FROM read_parquet('importer/canopy/data/temp/20260506-gbif-test-run1.parquet');
	""")
	# Load second run from current local release path.
	db.execute("""
		CREATE OR REPLACE TEMP TABLE drift_run2 AS
		SELECT id_meso, name_consensus, accepted, wikidata_id, gbif_id, col_id, inaturalist_id, accepted_by, considered_synonym
		FROM read_parquet('importer/canopy/data/releases/20260506-e738733531be/20260506-e738733531be.parquet');
	""")
	# Print examples where the same Meso UUID picked a different Wikidata row.
	db.sql("""
		SELECT
			r1.name_consensus,
			r1.id_meso,
			r1.accepted AS run1_accepted,
			r2.accepted AS run2_accepted,
			r1.wikidata_id AS run1_wikidata,
			w1.name_clean AS run1_wikiname,
			r2.wikidata_id AS run2_wikidata,
			w2.name_clean AS run2_wikiname,
			r1.gbif_id AS run1_gbif,
			r2.gbif_id AS run2_gbif,
			r1.col_id AS run1_col,
			r2.col_id AS run2_col,
			r1.inaturalist_id AS run1_inat,
			r2.inaturalist_id AS run2_inat,
			r1.accepted_by AS run1_accepted_by,
			r2.accepted_by AS run2_accepted_by,
			r1.considered_synonym AS run1_synonym,
			r2.considered_synonym AS run2_synonym
		FROM drift_run1 r1
		JOIN drift_run2 r2 USING (id_meso)
		LEFT JOIN wikidata w1 ON r1.wikidata_id = w1.id_raw
		LEFT JOIN wikidata w2 ON r2.wikidata_id = w2.id_raw
		WHERE r1.wikidata_id IS DISTINCT FROM r2.wikidata_id
		AND r1.name_consensus IN (
			SELECT name_consensus FROM drift_run2 WHERE accepted
			EXCEPT SELECT name_consensus FROM drift_run1 WHERE accepted
			UNION
			SELECT name_consensus FROM drift_run1 WHERE accepted
			EXCEPT SELECT name_consensus FROM drift_run2 WHERE accepted
		)
		ORDER BY r1.name_consensus
		LIMIT 20;
	""").show(max_rows=20, max_width=260)

# Run all probe phases.
def main():
	# Initialize local runtime.
	init_runtime()
	# Keep one in-memory database for the full probe.
	with duckdb.connect(':memory:') as db:
		# Route any DuckDB spill into canopy temp.
		db.execute(f"SET temp_directory = '{TMP_DIR}'")
		# Build initial backbone.
		section('building initial meso')
		# Time initial setup.
		start = time.time()
		# Load source tables and initial meso.
		build_initial_meso(db)
		# Ensure ID columns match production preconditions.
		ensure_id_columns(db)
		# Store base table for option copies.
		db.execute("CREATE OR REPLACE TEMP TABLE meso_base AS SELECT * FROM meso;")
		# Print setup timing.
		print(f"initial_meso_rows={db.execute('SELECT COUNT(*) FROM meso_base').fetchone()[0]:,} runtime={elapsed(start)}", flush=True)
		# Run three authority-ID matching options.
		section('wikidata authority-id matching')
		run_authority_option(db, 'current', 'current')
		run_authority_option(db, 'id_order', 'id_order')
		run_authority_option(db, 'name_aware', 'name_aware')
		# Run three name-fallback options paired with their matching authority strategy.
		section('wikidata exact-name fallback')
		run_name_fallback_option(db, 'current', 'current')
		run_name_fallback_option(db, 'id_order', 'id_order')
		run_name_fallback_option(db, 'name_aware', 'pagecount')
		# Apply production supplementation to expose downstream ID changes.
		section('wikidata source-id supplementation')
		supplement_from_wikidata(db, 'current')
		supplement_from_wikidata(db, 'id_order')
		supplement_from_wikidata(db, 'name_aware')
		# Compare options and print spot-check examples.
		compare_options(db)
		# Find more examples from current local drift.
		find_existing_drift_examples(db)

# Execute script entrypoint.
if __name__ == '__main__':
	# Run the abtest.
	main()
