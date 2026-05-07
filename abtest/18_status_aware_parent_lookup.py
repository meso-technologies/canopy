# Compare current parent flow with acceptance-first status-aware parent lookup.
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

# Sentinels from parent drift investigations.
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

# Build the expression used to count filled authority IDs on parent candidates.
def authority_count_expr(prefix='m_parent'):
	# Convert authority IDs to varchar so list_filter has one list type.
	values = []
	# Include core authorities because they define taxonomy provenance.
	for auth in fuse_pipeline.core_authorities:
		# Build column name once for type lookup.
		col = f'{auth}_id'
		# Cast every ID to varchar for a homogeneous list.
		values.append(f'CAST({prefix}.{col} AS VARCHAR)')
	# Count non-null authority IDs as a source-richness signal.
	return f"len(list_filter([{', '.join(values)}], lambda x: x IS NOT NULL))"

# Custom parent_raw vote that can use accepted_by and considered_synonym after decide_acceptance().
def status_aware_parent_vote(results, db, column, authorities=None):
	# Fall back on production authority list.
	if not authorities: authorities = fuse_pipeline.core_authorities
	# Preserve generic vote behavior for anything except this targeted test path.
	if column != 'parent_raw': return fuse_pipeline.vote(results, db, column, authorities)
	# Parent vote stores UUID values.
	fieldname = 'parent'
	# Collect authorities that actually expose parent_raw.
	pool = []
	# Build source-specific parent vote columns.
	for authority in authorities:
		# Skip authorities without parent_raw.
		if db.sql(f"SELECT column_name FROM information_schema.columns WHERE table_name = '{authority}' AND column_name = 'parent_raw'").fetchone() is None: continue
		# Keep this authority in vote order.
		pool.append(authority)
		# Add authority-specific parent UUID column.
		db.execute(f"ALTER TABLE meso ADD COLUMN IF NOT EXISTS parent_{authority} UUID;")
		# Compute source-richness expression for this subquery.
		authority_count = authority_count_expr('m_parent')
		# Resolve source parent IDs to deterministic parent candidates using acceptance evidence.
		db.execute(f"""
			UPDATE meso SET parent_{authority} = (
				-- Map this source row's parent_raw ID to one candidate Meso parent UUID
				SELECT m_parent.id_meso
				FROM {authority} AS i
				JOIN meso AS m_parent
				ON m_parent.{authority}_id IS NOT NULL
				AND i.parent_raw = m_parent.{authority}_id
				WHERE i.id_raw = meso.{authority}_id
				ORDER BY
					-- Prefer parent rows explicitly accepted by more source authorities
					COALESCE(len(m_parent.accepted_by), 0) DESC,
					-- Avoid parent rows considered synonyms by more source authorities
					COALESCE(len(m_parent.considered_synonym), 0) ASC,
					-- Prefer richer cross-source ID rows after source-status evidence
					{authority_count} DESC,
					-- Finish with stable UUID ordering so identical inputs produce identical outputs
					m_parent.id_meso::VARCHAR ASC
				LIMIT 1
			)
			WHERE EXISTS (SELECT 1 FROM {authority} WHERE parent_raw IS NOT NULL AND id_raw = meso.{authority}_id);
		""")
	# Add pool and consensus columns.
	db.execute(f"""
		ALTER TABLE meso ADD COLUMN IF NOT EXISTS parent_pool UUID[];
		ALTER TABLE meso ADD COLUMN IF NOT EXISTS parent_consensus UUID;
	""")
	# Build parent pool in production authority order.
	db.execute(f"UPDATE meso SET parent_pool = [{', '.join([f'parent_{authority}' for authority in pool])}]")
	# Use production list_mode so this option isolates parent lookup ordering first.
	db.execute("UPDATE meso SET parent_consensus = list_mode(parent_pool);")
	# Add value to votes held if it is not already present in this interpreter.
	if 'parent_consensus' not in fuse_pipeline.votes_held: fuse_pipeline.votes_held.append('parent_consensus')

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
	# Acceptance-first with current parent logic tests order alone.
	elif flow == 'acceptance_first_current':
		# Compute source acceptance evidence first.
		fuse_pipeline.decide_acceptance(results, db)
		# Compute parent_consensus with existing production parent lookup.
		fuse_pipeline.add_higher_ranks(results, db)
	# Acceptance-first with status-aware parent lookup tests the proposed combined direction.
	elif flow == 'acceptance_first_status':
		# Compute source acceptance evidence first.
		fuse_pipeline.decide_acceptance(results, db)
		# Temporarily replace only vote(parent_raw) while add_higher_ranks runs.
		original_vote = fuse_pipeline.vote
		# Install status-aware parent vote for this flow.
		fuse_pipeline.vote = status_aware_parent_vote
		try:
			# Compute parent_consensus using status-aware lookup and normal higher-rank derivation.
			fuse_pipeline.add_higher_ranks(results, db)
		finally:
			# Restore production vote helper immediately after this flow.
			fuse_pipeline.vote = original_vote
	# Abort on programming errors.
	else: raise RuntimeError(f'Unknown flow {flow}')
	# Snapshot this flow for comparison.
	db.execute(f"CREATE OR REPLACE TEMP TABLE meso_{flow} AS SELECT * FROM meso;")
	# Print runtime.
	print(f"flow {flow}: runtime={elapsed(start)}", flush=True)

# Print broad shape metrics for a flow.
def print_shape(db, flow):
	# Count accepted rows and provenance shape.
	row = db.execute(f"""
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
	""").fetchone()
	# Print compact shape line.
	print(
		f"{flow}: total={row[0]:,} accepted={row[1]:,} accepted_no_by={row[2]:,} accepted_with_syn={row[3]:,} "
		f"rows_with_parent={row[4]:,} accepted_with_parent={row[5]:,} dangling={row[6]:,} no_phylum={row[7]:,}",
		flush=True,
	)

# Print direct differences between flows.
def print_pairwise(db, left, right):
	# Compare major row-level fields by stable id_meso.
	row = db.execute(f"""
		SELECT
			COUNT(*) AS shared_rows,
			SUM((l.accepted IS DISTINCT FROM r.accepted)::INT) AS accepted_changed,
			SUM((l.accepted_by IS DISTINCT FROM r.accepted_by)::INT) AS accepted_by_changed,
			SUM((l.considered_synonym IS DISTINCT FROM r.considered_synonym)::INT) AS synonym_list_changed,
			SUM((l.parent_consensus IS DISTINCT FROM r.parent_consensus)::INT) AS parent_changed,
			SUM((pl.name_consensus = pr.name_consensus)::INT) FILTER (WHERE l.parent_consensus IS DISTINCT FROM r.parent_consensus) AS same_parent_name,
			SUM((pl.name_consensus IS DISTINCT FROM pr.name_consensus)::INT) FILTER (WHERE l.parent_consensus IS DISTINCT FROM r.parent_consensus) AS different_parent_name,
			SUM((l.phylum IS DISTINCT FROM r.phylum)::INT) AS phylum_changed,
			SUM((l.family IS DISTINCT FROM r.family)::INT) AS family_changed,
			SUM((l.genus IS DISTINCT FROM r.genus)::INT) AS genus_changed
		FROM meso_{left} l
		JOIN meso_{right} r USING (id_meso)
		LEFT JOIN meso_{left} pl ON l.parent_consensus = pl.id_meso
		LEFT JOIN meso_{right} pr ON r.parent_consensus = pr.id_meso;
	""").fetchone()
	# Compare accepted name sets.
	names = db.execute(f"""
		WITH left_names AS (SELECT DISTINCT name_consensus FROM meso_{left} WHERE accepted),
		     right_names AS (SELECT DISTINCT name_consensus FROM meso_{right} WHERE accepted)
		SELECT
			(SELECT COUNT(*) FROM right_names LEFT JOIN left_names USING(name_consensus) WHERE left_names.name_consensus IS NULL) AS added,
			(SELECT COUNT(*) FROM left_names LEFT JOIN right_names USING(name_consensus) WHERE right_names.name_consensus IS NULL) AS deleted,
			(SELECT COUNT(*) FROM left_names JOIN right_names USING(name_consensus)) AS common;
	""").fetchone()
	# Print compact pairwise line.
	print(
		f"{left} vs {right}: accepted_changed={row[1]:,} accepted_by_changed={row[2]:,} synonym_changed={row[3]:,} "
		f"parent_changed={row[4]:,} same_parent_name={row[5] or 0:,} different_parent_name={row[6] or 0:,} "
		f"phylum_changed={row[7]:,} family_changed={row[8]:,} genus_changed={row[9]:,} accepted_names=+{names[0]:,}/-{names[1]:,}/={names[2]:,}",
		flush=True,
	)

# Print top parent changes between two flows.
def print_top_parent_changes(db, left, right):
	# Announce top parent change table.
	section(f'Top parent changes {left} vs {right}')
	# Show largest parent-name/author/year changes.
	db.sql(f"""
		SELECT
			pl.name_consensus AS left_parent,
			pl.author_consensus AS left_author,
			pl.year_consensus AS left_year,
			pr.name_consensus AS right_parent,
			pr.author_consensus AS right_author,
			pr.year_consensus AS right_year,
			COUNT(*) AS rows_changed
		FROM meso_{left} l
		JOIN meso_{right} r USING (id_meso)
		LEFT JOIN meso_{left} pl ON l.parent_consensus = pl.id_meso
		LEFT JOIN meso_{right} pr ON r.parent_consensus = pr.id_meso
		WHERE l.parent_consensus IS DISTINCT FROM r.parent_consensus
		GROUP BY 1,2,3,4,5,6
		ORDER BY rows_changed DESC
		LIMIT 30;
	""").show(max_rows=30, max_width=180)

# Print sentinel rows for all flows.
def print_sentinels(db, flows):
	# Announce sentinel section.
	section('Sentinels')
	# Build SQL literal list once.
	names = ', '.join([repr(name) for name in SENTINELS])
	# Iterate flows in fixed order.
	for flow in flows:
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
	section('Status-aware parent lookup AB test')
	# Open an in-memory DuckDB database.
	with duckdb.connect(':memory:') as db:
		# Keep temporary spill files in canopy temp directory inherited from the process CWD.
		db.execute("SET temp_directory='importer/canopy/data/temp'")
		# Build shared base table once.
		results = build_base_state(db)
		# Snapshot base state for each flow.
		db.execute("CREATE OR REPLACE TEMP TABLE meso_base AS SELECT * FROM meso;")
		# Define flow names.
		flows = ['current', 'acceptance_first_current', 'acceptance_first_status']
		# Run every flow.
		for flow in flows: run_flow(db, results, flow)
		# Print broad shapes.
		section('Shape summary')
		for flow in flows: print_shape(db, flow)
		# Print pairwise comparisons.
		section('Pairwise differences')
		print_pairwise(db, 'current', 'acceptance_first_current')
		print_pairwise(db, 'current', 'acceptance_first_status')
		print_pairwise(db, 'acceptance_first_current', 'acceptance_first_status')
		# Print top changes for status-aware option against current production.
		print_top_parent_changes(db, 'current', 'acceptance_first_status')
		# Print sentinel rows.
		print_sentinels(db, flows)

# Run script when invoked directly.
if __name__ == '__main__':
	# Enter main routine.
	main()
