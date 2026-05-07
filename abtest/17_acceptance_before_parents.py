# Compare current parent-before-acceptance flow with acceptance-before-parent flow.
from types import SimpleNamespace
# Load timing helpers for flow runtime comparisons.
import time
# Load DuckDB for set-based A/B probes.
import duckdb
# Load canopy settings proxy and builder.
from importer.canopy import settings, build_settings
# Load latest processed-file discovery.
from importer.canopy.utils.filehandlers import get_latest_processed
# Load production fuse helpers and constants.
from importer.canopy.pipeline import fuse as fuse_pipeline

# Parent and acceptance sentinels from recent drift investigations.
SENTINELS = [
	# Previously unstable parent canonical case.
	'erica hanekomii',
	# Residual parent-name drift examples.
	'bazzania heterostipa',
	'sphagnum cymbifolium',
	'erophila verna',
	# Common acceptance/litmus names to ensure broad sanity.
	'salvia rosmarinus',
	'vaccinium vitisidaea',
]

# Print a compact section header.
def section(title):
	# Separate output sections in the console.
	print(f"\n############### {title} ###############", flush=True)

# Format elapsed runtime for one flow.
def elapsed(start):
	# Return seconds with one decimal place.
	return f"{time.time() - start:.1f}s"

# Initialize canopy settings for local hydrated files.
def init_runtime():
	# Build minimal CLI args for local, non-S3 analysis.
	args = SimpleNamespace(debug=False, verbose=False, force=False, csv=False, s3=False)
	# Install runtime settings into the canopy proxy.
	settings.set_config(build_settings(args))

# Build the same meso state production has immediately before current add_higher_ranks().
def build_base_state(db):
	# Resolve latest hydrated processed inputs.
	results = get_latest_processed()
	# Load source parquet tables into DuckDB using production code.
	fuse_pipeline.load_map_sources(results, db)
	# Build initial backbone rows using production code.
	fuse_pipeline.initial_backbone(results, db)
	# Add deterministic cross-source IDs before consensus and acceptance.
	fuse_pipeline.add_ids(results, db)
	# Build consensus names ranks authors years using production code.
	fuse_pipeline.basic_consensus(results, db)
	# Create Meso UUIDs before parent lookup.
	fuse_pipeline.create_hashes(results, db)
	# Load enrichment sources because Wikidata acceptance uses pagecount.
	fuse_pipeline.load_enrich_sources(results, db)
	# Apply enrichment exactly as production does.
	fuse_pipeline.enrich(results, db)
	# Reduce vernacular because production does it before parent/acceptance stages.
	fuse_pipeline.reduce_vernacular(results, db)
	# Return source inventory for running production helpers.
	return results

# Run one flow from a copied base table.
def run_flow(db, results, flow):
	# Start runtime timer.
	start = time.time()
	# Restore the shared base state into the production table name expected by helper functions.
	db.execute("CREATE OR REPLACE TEMP TABLE meso AS SELECT * FROM meso_base;")
	# Current production order computes parents and higher ranks before acceptance evidence.
	if flow == 'current':
		# Compute parent_consensus and higher-rank columns first.
		fuse_pipeline.add_higher_ranks(results, db)
		# Compute accepted_by considered_synonym synonym and accepted flags second.
		fuse_pipeline.decide_acceptance(results, db)
	# Candidate order computes acceptance evidence before parent choices.
	elif flow == 'acceptance_first':
		# Compute accepted_by considered_synonym synonym and accepted flags first.
		fuse_pipeline.decide_acceptance(results, db)
		# Compute parent_consensus and higher-rank columns after evidence exists.
		fuse_pipeline.add_higher_ranks(results, db)
	# Abort on programming errors.
	else: raise RuntimeError(f'Unknown flow {flow}')
	# Snapshot this flow for comparison.
	db.execute(f"CREATE OR REPLACE TEMP TABLE meso_{flow} AS SELECT * FROM meso;")
	# Print runtime.
	print(f"flow {flow}: runtime={elapsed(start)}", flush=True)

# Print broad shape metrics for a flow.
def print_shape(db, flow):
	# Announce flow shape.
	section(f'Shape {flow}')
	# Count accepted rows and provenance shape.
	db.sql(f"""
		SELECT
			COUNT(*) AS total_rows,
			SUM(accepted::INT) AS accepted_rows,
			SUM((accepted AND accepted_by IS NULL)::INT) AS accepted_without_accepted_by,
			SUM((accepted AND considered_synonym IS NOT NULL)::INT) AS accepted_with_synonym_signal,
			SUM((parent_consensus IS NOT NULL)::INT) AS rows_with_parent,
			SUM((accepted AND parent_consensus IS NOT NULL)::INT) AS accepted_with_parent,
			SUM((accepted AND parent_consensus IS NOT NULL AND parent_consensus NOT IN (SELECT id_meso FROM meso_{flow}))::INT) AS accepted_dangling_parent,
			SUM((rank_consensus != 'KINGDOM' AND phylum IS NULL)::INT) AS non_kingdom_without_phylum
		FROM meso_{flow};
	""").show(max_width=180)
	# Count accepted rows by kingdom and rank family.
	db.sql(f"""
		SELECT kingdom, rank_consensus, COUNT(*) AS accepted_rows
		FROM meso_{flow}
		WHERE accepted
		GROUP BY 1,2
		ORDER BY accepted_rows DESC
		LIMIT 20;
	""").show(max_rows=20)

# Print direct differences between the two flows.
def print_differences(db):
	# Announce comparison section.
	section('Flow differences')
	# Compare major row-level fields by stable id_meso.
	db.sql("""
		SELECT
			COUNT(*) AS shared_rows,
			SUM((c.accepted IS DISTINCT FROM a.accepted)::INT) AS accepted_changed,
			SUM((c.accepted_by IS DISTINCT FROM a.accepted_by)::INT) AS accepted_by_changed,
			SUM((c.considered_synonym IS DISTINCT FROM a.considered_synonym)::INT) AS synonym_list_changed,
			SUM((c.parent_consensus IS DISTINCT FROM a.parent_consensus)::INT) AS parent_changed,
			SUM((c.phylum IS DISTINCT FROM a.phylum)::INT) AS phylum_changed,
			SUM((c.family IS DISTINCT FROM a.family)::INT) AS family_changed,
			SUM((c.genus IS DISTINCT FROM a.genus)::INT) AS genus_changed
		FROM meso_current c
		JOIN meso_acceptance_first a USING (id_meso);
	""").show(max_width=180)
	# Compare accepted name sets.
	db.sql("""
		WITH cur AS (SELECT DISTINCT name_consensus FROM meso_current WHERE accepted),
		     acc AS (SELECT DISTINCT name_consensus FROM meso_acceptance_first WHERE accepted)
		SELECT
			(SELECT COUNT(*) FROM acc LEFT JOIN cur USING(name_consensus) WHERE cur.name_consensus IS NULL) AS accepted_names_added,
			(SELECT COUNT(*) FROM cur LEFT JOIN acc USING(name_consensus) WHERE acc.name_consensus IS NULL) AS accepted_names_deleted,
			(SELECT COUNT(*) FROM cur JOIN acc USING(name_consensus)) AS accepted_names_common;
	""").show()
	# Summarize parent-name differences.
	db.sql("""
		WITH changed AS (
			SELECT c.id_meso, c.name_consensus,
				pc.name_consensus AS current_parent_name,
				pa.name_consensus AS acceptance_first_parent_name
			FROM meso_current c
			JOIN meso_acceptance_first a USING (id_meso)
			LEFT JOIN meso_current pc ON c.parent_consensus = pc.id_meso
			LEFT JOIN meso_acceptance_first pa ON a.parent_consensus = pa.id_meso
			WHERE c.parent_consensus IS DISTINCT FROM a.parent_consensus
		)
		SELECT
			COUNT(*) AS parent_changed,
			SUM((current_parent_name = acceptance_first_parent_name)::INT) AS same_parent_name,
			SUM((current_parent_name IS DISTINCT FROM acceptance_first_parent_name)::INT) AS different_parent_name
		FROM changed;
	""").show()

# Print top parent changes between flows.
def print_top_parent_changes(db):
	# Announce top parent change table.
	section('Top parent changes current vs acceptance_first')
	# Show largest parent-name/author/year changes.
	db.sql("""
		SELECT
			pc.name_consensus AS current_parent,
			pc.author_consensus AS current_author,
			pc.year_consensus AS current_year,
			pa.name_consensus AS acceptance_first_parent,
			pa.author_consensus AS acceptance_first_author,
			pa.year_consensus AS acceptance_first_year,
			COUNT(*) AS rows_changed
		FROM meso_current c
		JOIN meso_acceptance_first a USING (id_meso)
		LEFT JOIN meso_current pc ON c.parent_consensus = pc.id_meso
		LEFT JOIN meso_acceptance_first pa ON a.parent_consensus = pa.id_meso
		WHERE c.parent_consensus IS DISTINCT FROM a.parent_consensus
		GROUP BY 1,2,3,4,5,6
		ORDER BY rows_changed DESC
		LIMIT 30;
	""").show(max_rows=30, max_width=180)

# Print sentinel rows for both flows.
def print_sentinels(db):
	# Announce sentinel section.
	section('Sentinels')
	# Build SQL literal list once.
	names = ', '.join([repr(name) for name in SENTINELS])
	# Iterate flows in fixed order.
	for flow in ['current', 'acceptance_first']:
		# Print flow header.
		print(f"\n{flow}", flush=True)
		# Show sentinel parent and acceptance fields.
		db.sql(f"""
			SELECT
				m.name_consensus,
				m.rank_consensus,
				m.accepted,
				m.accepted_by,
				m.considered_synonym,
				p.name_consensus AS parent_name,
				p.author_consensus AS parent_author,
				p.year_consensus AS parent_year,
				p.rank_consensus AS parent_rank
			FROM meso_{flow} m
			LEFT JOIN meso_{flow} p ON m.parent_consensus = p.id_meso
			WHERE m.name_consensus IN ({names})
			ORDER BY m.name_consensus, m.id_meso;
		""").show(max_rows=60, max_width=220)

# Main script entry point.
def main():
	# Initialize local canopy runtime.
	init_runtime()
	# Announce test.
	section('Acceptance before parents AB test')
	# Open an in-memory DuckDB database.
	with duckdb.connect(':memory:') as db:
		# Keep temporary spill files in canopy temp directory inherited from the process CWD.
		db.execute("SET temp_directory='importer/canopy/data/temp'")
		# Build shared base table once.
		results = build_base_state(db)
		# Snapshot base state for each flow.
		db.execute("CREATE OR REPLACE TEMP TABLE meso_base AS SELECT * FROM meso;")
		# Run current production order.
		run_flow(db, results, 'current')
		# Run candidate order.
		run_flow(db, results, 'acceptance_first')
		# Print broad shapes.
		print_shape(db, 'current')
		print_shape(db, 'acceptance_first')
		# Print direct differences.
		print_differences(db)
		# Print top parent changes.
		print_top_parent_changes(db)
		# Print sentinel rows.
		print_sentinels(db)

# Run script when invoked directly.
if __name__ == '__main__':
	# Enter main routine.
	main()
