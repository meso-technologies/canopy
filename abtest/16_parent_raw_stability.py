# Compare parent_raw lookup strategies before production parent consensus voting.
from types import SimpleNamespace
# Load timing helpers for runtime comparisons.
import time
# Load DuckDB for set-based A/B probes.
import duckdb
# Load canopy settings proxy and builder.
from importer.canopy import settings, build_settings
# Load latest processed-file discovery.
from importer.canopy.utils.filehandlers import get_latest_processed
# Load production fuse helpers and constants.
from importer.canopy.pipeline import fuse as fuse_pipeline

# Child names from residual parent drift examples after parent canonical stabilization.
SENTINELS = [
	# Large observed parent-name flip group.
	'bazzania heterostipa',
	# Large observed duplicate genus-author flip group.
	'dryandra',
	# Large observed duplicate genus-author flip group.
	'schmidelia',
	# Previously fixed parent canonical sentinel should remain useful context.
	'erica hanekomii',
	# Observed genus/species parent inversion group.
	'sphagnum cymbifolium',
	# Observed genus/species parent inversion group.
	'erophila verna',
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

# Build the same meso table state production has immediately before add_higher_ranks().
def build_pre_parent_raw_state(db):
	# Resolve latest hydrated processed inputs.
	results = get_latest_processed()
	# Load source parquet tables into DuckDB using production code.
	fuse_pipeline.load_map_sources(results, db)
	# Build initial backbone rows using production code.
	fuse_pipeline.initial_backbone(results, db)
	# Add deterministic cross-source IDs before consensus and parent lookup.
	fuse_pipeline.add_ids(results, db)
	# Build consensus names ranks authors years using production code.
	fuse_pipeline.basic_consensus(results, db)
	# Create Meso UUIDs because parent_raw maps source parent IDs to these UUIDs.
	fuse_pipeline.create_hashes(results, db)
	# Load enrichment sources because production runs enrichment before parent_raw voting.
	fuse_pipeline.load_enrich_sources(results, db)
	# Apply enrichment so parent candidate rows have the same support fields as production.
	fuse_pipeline.enrich(results, db)
	# Reduce vernacular because production runs it before add_higher_ranks.
	fuse_pipeline.reduce_vernacular(results, db)
	# Return source inventory for later source-column checks.
	return results

# Build the expression used to count filled authority IDs on parent candidates.
def authority_count_expr(prefix='m_parent'):
	# Convert integer authority IDs to varchar so list_filter has one list type.
	values = []
	# Include core authorities because they define taxonomy provenance.
	for auth in fuse_pipeline.core_authorities:
		# Build column name once for type lookup.
		col = f'{auth}_id'
		# Cast integer IDs to varchar before list construction.
		if col in fuse_pipeline.int_ids: values.append(f'CAST({prefix}.{col} AS VARCHAR)')
		# Keep varchar IDs as-is.
		else: values.append(f'{prefix}.{col}')
	# Count non-null authority IDs as a source-richness signal.
	return f"len(list_filter([{', '.join(values)}], lambda x: x IS NOT NULL))"

# Return authorities that have parent_raw available.
def parent_raw_authorities(db):
	# Start with no eligible authorities.
	pool = []
	# Match production parent_raw vote authorities.
	for authority in fuse_pipeline.core_authorities:
		# Check whether this source table has parent_raw.
		row = db.sql(f"SELECT column_name FROM information_schema.columns WHERE table_name = '{authority}' AND column_name = 'parent_raw'").fetchone()
		# Include authorities with parent_raw only.
		if row is not None: pool.append(authority)
	# Return ordered production pool.
	return pool

# Return deterministic ORDER BY for one parent_raw lookup strategy.
def lookup_order(strategy):
	# Current production behavior has no ordering and is intentionally left blank.
	if strategy == 'current': return ''
	# Authority strategy chooses the source-richest parent candidate and stable UUID tie-break.
	if strategy == 'authority_order': return f"ORDER BY {authority_count_expr('m_parent')} DESC, m_parent.id_meso::VARCHAR ASC"
	# Rank-aware strategy rejects child-or-self rank inversions before source richness.
	if strategy == 'rank_aware': return f"ORDER BY CASE WHEN m_parent.rank_consensus < meso_{strategy}.rank_consensus THEN 0 ELSE 1 END, {authority_count_expr('m_parent')} DESC, m_parent.id_meso::VARCHAR ASC"
	# Abort on programming errors.
	raise RuntimeError(f'Unknown strategy {strategy}')

# Apply one parent_raw lookup and vote strategy to a meso copy.
def run_strategy(db, strategy, pool):
	# Start runtime timer.
	start = time.time()
	# Copy the same pre-parent table for this option.
	db.execute(f"CREATE OR REPLACE TEMP TABLE meso_{strategy} AS SELECT * FROM meso_base;")
	# Add parent vote target columns matching production vote().
	db.execute(f"""
		ALTER TABLE meso_{strategy} ADD COLUMN IF NOT EXISTS parent_pool UUID[];
		ALTER TABLE meso_{strategy} ADD COLUMN IF NOT EXISTS parent_consensus UUID;
	""")
	# Map each source parent_raw value to one Meso parent UUID.
	for authority in pool:
		# Add authority-specific parent vote column.
		db.execute(f"ALTER TABLE meso_{strategy} ADD COLUMN IF NOT EXISTS parent_{authority} UUID;")
		# Build optional deterministic order clause.
		order = lookup_order(strategy)
		# Apply scalar lookup shape so timings match the production query family.
		db.execute(f"""
			UPDATE meso_{strategy} SET parent_{authority} = (
				-- Map this source row's parent_raw ID to one candidate Meso parent UUID
				SELECT m_parent.id_meso
				FROM {authority} AS i
				JOIN meso_{strategy} AS m_parent
				ON m_parent.{authority}_id IS NOT NULL
				AND i.parent_raw = m_parent.{authority}_id
				WHERE i.id_raw = meso_{strategy}.{authority}_id
				{order}
				LIMIT 1
			)
			WHERE EXISTS (SELECT 1 FROM {authority} WHERE parent_raw IS NOT NULL AND id_raw = meso_{strategy}.{authority}_id);
		""")
	# Build parent vote pools in production authority order.
	db.execute(f"UPDATE meso_{strategy} SET parent_pool = [{', '.join([f'parent_{authority}' for authority in pool])}];")
	# Use current production consensus voting so this test isolates lookup ordering first.
	db.execute(f"UPDATE meso_{strategy} SET parent_consensus = list_mode(parent_pool);")
	# Count rows with parent consensus.
	assigned = db.execute(f"SELECT COUNT(parent_consensus) FROM meso_{strategy};").fetchone()[0]
	# Count ambiguous parent pool rows after lookup.
	ambiguous = db.execute(f"SELECT COUNT(*) FROM meso_{strategy} WHERE len(list_distinct(parent_pool)) > 1;").fetchone()[0]
	# Print strategy summary.
	print(f"strategy {strategy}: assigned={assigned:,} ambiguous_parent_pools={ambiguous:,} runtime={elapsed(start)}", flush=True)

# Count source parent mappings where the source parent ID maps to multiple Meso candidates.
def print_ambiguity_counts(db, pool):
	# Announce ambiguity count section.
	section('Source parent ID ambiguity')
	# Iterate authorities in production order.
	for authority in pool:
		# Count source parent IDs that have more than one possible Meso parent row.
		row = db.execute(f"""
			WITH parent_candidates AS (
				-- Project candidate rows for each source parent ID.
				SELECT i.parent_raw, COUNT(DISTINCT m_parent.id_meso) AS candidate_count
				FROM {authority} i
				JOIN meso_base m_parent ON m_parent.{authority}_id IS NOT NULL AND i.parent_raw = m_parent.{authority}_id
				WHERE i.parent_raw IS NOT NULL
				GROUP BY i.parent_raw
			)
			SELECT
				COUNT(*) AS parent_ids_with_candidates,
				SUM((candidate_count > 1)::INT) AS ambiguous_parent_ids,
				MAX(candidate_count) AS max_candidates
			FROM parent_candidates;
		""").fetchone()
		# Normalize empty authorities so formatting does not fail on NULL aggregate values.
		parent_ids = row[0] or 0
		# Normalize missing ambiguity count to zero.
		ambiguous = row[1] or 0
		# Normalize missing maximum candidate count to zero.
		max_candidates = row[2] or 0
		# Print authority ambiguity summary.
		print(f"{authority}: parent_ids={parent_ids:,} ambiguous={ambiguous:,} max_candidates={max_candidates:,}", flush=True)

# Print pairwise parent consensus differences.
def print_pairwise(db, strategies):
	# Announce pairwise section.
	section('Pairwise parent consensus differences')
	# Compare every strategy pair once.
	for i, left in enumerate(strategies):
		# Avoid duplicate pair output.
		for right in strategies[i + 1:]:
			# Count rows whose chosen parent UUID differs.
			row = db.execute(f"""
				SELECT
					COUNT(*) AS changed,
					SUM((pl.name_consensus = pr.name_consensus)::INT) AS same_parent_name,
					SUM((pl.name_consensus IS DISTINCT FROM pr.name_consensus)::INT) AS different_parent_name
				FROM meso_{left} l
				JOIN meso_{right} r USING (id_meso)
				LEFT JOIN meso_{left} pl ON l.parent_consensus = pl.id_meso
				LEFT JOIN meso_{right} pr ON r.parent_consensus = pr.id_meso
				WHERE l.parent_consensus IS DISTINCT FROM r.parent_consensus;
			""").fetchone()
			# Print pair result.
			print(f"{left} vs {right}: changed={row[0]:,} same_parent_name={row[1]:,} different_parent_name={row[2]:,}", flush=True)

# Print top parent-name changes between current and a candidate strategy.
def print_top_changes(db, strategy):
	# Announce top-change table.
	section(f'Top current vs {strategy} parent-name changes')
	# Show the largest parent-name pair differences.
	db.sql(f"""
		SELECT
			pc.name_consensus AS current_parent_name,
			pc.author_consensus AS current_author,
			pc.year_consensus AS current_year,
			pn.name_consensus AS new_parent_name,
			pn.author_consensus AS new_author,
			pn.year_consensus AS new_year,
			COUNT(*) AS rows_changed
		FROM meso_current c
		JOIN meso_{strategy} n USING (id_meso)
		LEFT JOIN meso_current pc ON c.parent_consensus = pc.id_meso
		LEFT JOIN meso_{strategy} pn ON n.parent_consensus = pn.id_meso
		WHERE c.parent_consensus IS DISTINCT FROM n.parent_consensus
		GROUP BY 1,2,3,4,5,6
		ORDER BY rows_changed DESC
		LIMIT 30;
	""").show(max_rows=30, max_width=180)

# Print sentinel parent choices for each strategy.
def print_sentinels(db, strategies):
	# Announce sentinel section.
	section('Sentinel parent choices')
	# Build SQL literal list once.
	names = ', '.join([repr(name) for name in SENTINELS])
	# Iterate strategies in fixed order.
	for strategy in strategies:
		# Print strategy header.
		print(f"\n{strategy}", flush=True)
		# Show parent choices for sentinel child names.
		db.sql(f"""
			SELECT
				m.name_consensus AS child,
				m.rank_consensus AS child_rank,
				p.name_consensus AS parent,
				p.author_consensus AS parent_author,
				p.year_consensus AS parent_year,
				p.rank_consensus AS parent_rank,
				m.parent_consensus
			FROM meso_{strategy} m
			LEFT JOIN meso_{strategy} p ON m.parent_consensus = p.id_meso
			WHERE m.name_consensus IN ({names})
			ORDER BY child, parent, parent_author;
		""").show(max_rows=40, max_width=220)

# Main script entry point.
def main():
	# Initialize local canopy runtime.
	init_runtime()
	# Announce test.
	section('Parent raw lookup AB test')
	# Open an in-memory DuckDB database.
	with duckdb.connect(':memory:') as db:
		# Keep temporary spill files in canopy temp directory inherited from the process CWD.
		db.execute("SET temp_directory='importer/canopy/data/temp'")
		# Build the real production state immediately before add_higher_ranks.
		build_pre_parent_raw_state(db)
		# Snapshot base state once so every option starts identical.
		db.execute("CREATE OR REPLACE TEMP TABLE meso_base AS SELECT * FROM meso;")
		# Resolve authorities with parent_raw.
		pool = parent_raw_authorities(db)
		# Print source ambiguity counts before strategy timing.
		print_ambiguity_counts(db, pool)
		# Define the three required strategy options.
		strategies = ['current', 'authority_order', 'rank_aware']
		# Announce strategy timing section.
		section('Strategy timings')
		# Run each strategy.
		for strategy in strategies: run_strategy(db, strategy, pool)
		# Print pairwise differences.
		print_pairwise(db, strategies)
		# Print top current-vs-candidate changes.
		print_top_changes(db, 'authority_order')
		# Print top current-vs-rank-aware changes.
		print_top_changes(db, 'rank_aware')
		# Print sentinel choices.
		print_sentinels(db, strategies)

# Run script when invoked directly.
if __name__ == '__main__':
	# Enter main routine.
	main()
