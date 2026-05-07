# Compare parent canonical selection strategies against the latest local canopy release.
from types import SimpleNamespace
# Load timing helpers for runtime comparisons.
import time
# Load DuckDB for set-based candidate probes.
import duckdb
# Load canopy settings proxy and builder.
from importer.canopy import settings, build_settings
# Load latest processed-file discovery.
from importer.canopy.utils.filehandlers import get_latest_processed
# Load production fuse constants and helpers for building the real pre-parent state.
from importer.canopy.pipeline import fuse as fuse_pipeline

# Parent names that have shown false canonical UUID churn in recent releases.
SENTINELS = [
	# Erica flips between an authority-rich botanical parent and a sparse sibling parent.
	'erica',
	# Keep a few high-child-count genera visible for spot checking if they are duplicated.
	'cortinarius',
	'carex',
	'solanum',
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

# Build the same meso table state production has right before consolidate_parents().
def build_pre_parent_state(db):
	# Resolve latest hydrated processed inputs.
	results = get_latest_processed()
	# Load source parquet tables into DuckDB using production code.
	fuse_pipeline.load_map_sources(results, db)
	# Build initial backbone rows using production code.
	fuse_pipeline.initial_backbone(results, db)
	# Add cross-source IDs using production deterministic ID logic.
	fuse_pipeline.add_ids(results, db)
	# Build consensus names/ranks/authors using production code.
	fuse_pipeline.basic_consensus(results, db)
	# Create stable Meso UUIDs before parent processing.
	fuse_pipeline.create_hashes(results, db)
	# Load enrichment sources because later pre-parent steps depend on enriched columns.
	fuse_pipeline.load_enrich_sources(results, db)
	# Apply enrichment exactly as production does.
	fuse_pipeline.enrich(results, db)
	# Reduce vernacular names because production runs this before higher-rank parent logic.
	fuse_pipeline.reduce_vernacular(results, db)
	# Build parent_consensus and higher ranks using production code.
	fuse_pipeline.add_higher_ranks(results, db)
	# Decide accepted/synonym status before name and parent dedupe.
	fuse_pipeline.decide_acceptance(results, db)
	# Run the first accepted-name dedupe before parent canonicalization.
	fuse_pipeline.dedupe_names(results, db)
	# Return source inventory for logging if needed later.
	return results

# Build the expression used to count filled authority IDs.
def authority_count_expr(prefix='m'):
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
	# Count non-null authority IDs as a compact source-richness signal.
	return f"len(list_filter([{', '.join(values)}], lambda x: x IS NOT NULL))"

# Create common parent-reference temp tables shared by all options.
def build_parent_tables(db):
	# Build the authority richness expression for parent rows.
	authority_count = authority_count_expr('m')
	# Materialize referenced parent IDs first to avoid broad self-joins over all taxa.
	db.execute(f"""
		CREATE OR REPLACE TEMP TABLE parent_refs AS
			-- Count how many accepted/final rows currently point at each parent UUID.
			SELECT parent_consensus AS id_meso, COUNT(*) AS ref_count
			FROM meso
			WHERE parent_consensus IS NOT NULL
			GROUP BY parent_consensus;
	""")
	# Materialize only rows that are actually referenced as parents.
	db.execute(f"""
		CREATE OR REPLACE TEMP TABLE parent_rows AS
			SELECT
				-- Parent UUID candidate.
				m.id_meso,
				-- Parent display/name key that is deduplicated by consolidate_parents.
				m.name_consensus,
				-- Keep author/year visible for sentinel review.
				m.author_consensus,
				m.year_consensus,
				-- Rank helps catch nonsensical parent choices.
				m.rank_consensus,
				-- Accepted-by count prefers rows supported by more taxonomy authorities.
				COALESCE(len(m.accepted_by), 0) AS acceptance_count,
				-- Synonym count penalizes rows marked synonym by authorities.
				COALESCE(len(m.considered_synonym), 0) AS synonym_count,
				-- Authority count prefers richer cross-source ID rows.
				{authority_count} AS authority_count,
				-- Reference count shows how much child traffic currently points at the row.
				r.ref_count
			FROM meso m
			JOIN parent_refs r USING (id_meso);
	""")
	# Restrict ranking to names that have more than one referenced parent UUID.
	db.execute("""
		CREATE OR REPLACE TEMP TABLE duplicate_parent_names AS
			-- Parent canonicalization only matters where one parent name has multiple UUIDs.
			SELECT name_consensus
			FROM parent_rows
			GROUP BY name_consensus
			HAVING COUNT(DISTINCT id_meso) > 1;
	""")
	# Print table sizes so accidental cartesian blowups are obvious.
	counts = db.execute("""
		SELECT
			(SELECT COUNT(*) FROM parent_refs) AS referenced_parent_ids,
			(SELECT COUNT(*) FROM parent_rows) AS parent_rows,
			(SELECT COUNT(*) FROM duplicate_parent_names) AS duplicate_parent_names;
	""").fetchone()
	# Show compact shape summary.
	print(f"parent tables: referenced_parent_ids={counts[0]:,} parent_rows={counts[1]:,} duplicate_parent_names={counts[2]:,}", flush=True)

# Return the ORDER BY fragment for one strategy.
def strategy_order(strategy):
	# Current option mirrors production MODE behavior and only adds UUID order for reproducible table output.
	if strategy == 'current_mode': return 'current_mode_id::VARCHAR ASC'
	# Authority-first option prefers broad source support before inherited reference count.
	if strategy == 'authority_first': return 'acceptance_count DESC, authority_count DESC, synonym_count ASC, ref_count DESC, id_meso::VARCHAR ASC'
	# Reference-first option trusts the already-converged child references before broader source richness.
	if strategy == 'ref_first': return 'ref_count DESC, acceptance_count DESC, authority_count DESC, synonym_count ASC, id_meso::VARCHAR ASC'
	# Abort on programming errors.
	raise RuntimeError(f'Unknown strategy {strategy}')

# Build one canonical selection option and print quality metrics.
def run_option(db, strategy):
	# Start runtime timer.
	start = time.time()
	# Current mode needs a separate canonical table because MODE is the behavior under test.
	if strategy == 'current_mode':
		# Build current canonical choices from referenced duplicate parent names.
		db.execute("""
			CREATE OR REPLACE TEMP TABLE canonical_current_mode AS
				-- Match current production behavior exactly for canonical UUID choice.
				SELECT pr.name_consensus, MODE(pr.id_meso) AS canonical_id
				FROM parent_rows pr
				JOIN duplicate_parent_names d USING (name_consensus)
				GROUP BY pr.name_consensus;
		""")
		# Add row metrics to make the output comparable to ranked strategies.
		db.execute("""
			CREATE OR REPLACE TEMP TABLE canonical_current_mode_full AS
				SELECT c.name_consensus, c.canonical_id, p.*, c.canonical_id AS current_mode_id
				FROM canonical_current_mode c
				JOIN parent_rows p ON p.id_meso = c.canonical_id;
		""")
		# Rank the mode result only to reuse common reporting SQL.
		db.execute("""
			CREATE OR REPLACE TEMP TABLE choice_current_mode AS
				SELECT * FROM canonical_current_mode_full;
		""")
	# Ranked options use explicit row_number ordering over only duplicated parent groups.
	else:
		# Add current mode ID so strategies can report how often they differ from current behavior.
		db.execute("""
			CREATE OR REPLACE TEMP TABLE current_mode_lookup AS
				SELECT pr.name_consensus, MODE(pr.id_meso) AS current_mode_id
				FROM parent_rows pr
				JOIN duplicate_parent_names d USING (name_consensus)
				GROUP BY pr.name_consensus;
		""")
		# Rank candidate parents using the requested deterministic strategy.
		db.execute(f"""
			CREATE OR REPLACE TEMP TABLE choice_{strategy} AS
				WITH ranked AS (
					SELECT
						-- Keep candidate metrics for reporting.
						p.*,
						-- Include current mode for changed-choice counts.
						c.current_mode_id,
						-- Pick one canonical parent per duplicated parent name.
						ROW_NUMBER() OVER (PARTITION BY p.name_consensus ORDER BY {strategy_order(strategy)}) AS rn
					FROM parent_rows p
					JOIN duplicate_parent_names d USING (name_consensus)
					JOIN current_mode_lookup c USING (name_consensus)
				)
				SELECT *, id_meso AS canonical_id FROM ranked WHERE rn = 1;
		""")
	# Collect aggregate metrics for the chosen parents.
	metrics = db.execute(f"""
		WITH choices AS (SELECT * FROM choice_{strategy}),
		rewrites AS (
			-- Count child references that would be rewritten to the selected canonical UUID.
			SELECT c.name_consensus, SUM(CASE WHEN pr.id_meso = c.canonical_id THEN 0 ELSE pr.ref_count END) AS rewrite_count
			FROM choices c
			JOIN parent_rows pr USING (name_consensus)
			GROUP BY c.name_consensus
		)
		SELECT
			COUNT(*) AS canonical_groups,
			SUM(rewrite_count) AS implied_child_rewrites,
			SUM((canonical_id IS DISTINCT FROM current_mode_id)::INT) AS differs_from_current,
			ROUND(AVG(acceptance_count), 3) AS avg_acceptance_count,
			ROUND(AVG(authority_count), 3) AS avg_authority_count,
			ROUND(AVG(synonym_count), 3) AS avg_synonym_count,
			SUM((acceptance_count = 0)::INT) AS zero_acceptance_choices
		FROM choices c
		JOIN rewrites r USING (name_consensus);
	""").fetchone()
	# Print one compact runtime/result line.
	print(
		f"option {strategy}: groups={metrics[0]:,} rewrites={metrics[1]:,} differs_from_current={metrics[2]:,} "
		f"avg_acceptance={metrics[3]} avg_authority={metrics[4]} avg_synonym={metrics[5]} zero_acceptance={metrics[6]:,} runtime={elapsed(start)}",
		flush=True,
	)
	# Return metrics for later comparison.
	return metrics

# Print sentinel choices for each strategy.
def print_sentinels(db, strategies):
	# Announce sentinel detail table.
	section('Sentinel choices')
	# Iterate strategies in fixed order.
	for strategy in strategies:
		# Print strategy header.
		print(f"\n{strategy}", flush=True)
		# Show selected canonical rows for sentinel parent names.
		db.sql(f"""
			SELECT
				name_consensus,
				canonical_id,
				author_consensus,
				year_consensus,
				rank_consensus,
				acceptance_count,
				authority_count,
				synonym_count,
				ref_count
			FROM choice_{strategy}
			WHERE name_consensus IN ({', '.join([repr(s) for s in SENTINELS])})
			ORDER BY name_consensus;
		""").show(max_rows=20, max_width=220)

# Print pairwise differences between strategies.
def print_pairwise(db, strategies):
	# Announce pairwise comparison section.
	section('Pairwise strategy differences')
	# Compare every strategy pair once.
	for i, left in enumerate(strategies):
		# Avoid duplicate pair output.
		for right in strategies[i + 1:]:
			# Count canonical group differences.
			count = db.execute(f"""
				SELECT COUNT(*)
				FROM choice_{left} l
				JOIN choice_{right} r USING (name_consensus)
				WHERE l.canonical_id IS DISTINCT FROM r.canonical_id;
			""").fetchone()[0]
			# Print pair result.
			print(f"{left} vs {right}: {count:,} parent-name choices differ", flush=True)

# Main script entry point.
def main():
	# Initialize local canopy runtime.
	init_runtime()
	# Print target stage.
	section('Parent canonical AB test before consolidate_parents')
	# Open an in-memory DuckDB database.
	with duckdb.connect(':memory:') as db:
		# Keep temporary spill files in canopy temp directory inherited from the process CWD.
		db.execute("SET temp_directory='importer/canopy/data/temp'")
		# Build the real production state immediately before parent canonicalization.
		build_pre_parent_state(db)
		# Build common parent candidate tables.
		build_parent_tables(db)
		# Define tested strategies.
		strategies = ['current_mode', 'authority_first', 'ref_first']
		# Announce option timings.
		section('Option timings')
		# Run every option.
		for strategy in strategies: run_option(db, strategy)
		# Print pairwise choice differences.
		print_pairwise(db, strategies)
		# Print sentinel choices.
		print_sentinels(db, strategies)

# Run script when invoked directly.
if __name__ == '__main__':
	# Enter main routine.
	main()
