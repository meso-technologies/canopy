# Localize and benchmark deterministic-tie-break candidates for residual fuse non-determinism.
#
# ###############################################################################
# # THIS PROBE FAILED ITS PURPOSE — 2026-05-09
# #
# # The Phase B bench produced false-positive "deterministic=NO" verdicts for
# # list_mode and mode, which are actually deterministic per-row. Acting on those
# # verdicts led to multi-hour wild goose chases and seven fuse.py edits that all
# # got reverted.
# #
# # Root cause: result_hash() uses string_agg(... ORDER BY key) but the key is not
# # unique in the candidate output tables (LEFT JOINs to authority parquets multiply
# # rows). Under DuckDB parallel execution, string_agg ties on equal sort keys
# # interleave in non-deterministic order across runs, producing different md5 hashes
# # for IDENTICAL output. See docs/agents/reference/duckdb.md "Determinism gotchas
# # under parallel execution" for the reusable lesson.
# #
# # Full session post-mortem and corrected next-step plan:
# #   docs/agents/journal/2026-05-09-1700-fuse-determinism-bench-was-wrong.md
# #   docs/agents/handovers/taxonomy-parent-canonical-stability.md ("2026-05-09
# #   second pass" section)
# #
# # Phase A (--locate) is still useful: subprocess fuse + set-based EXCEPT diff on
# # accepted (name_consensus, kingdom) pairs measures real cross-process drift
# # independent of any in-process measurement bug. The handover suggests the proper
# # next investigation is staged in-process hashing inside fuse.py itself, not more
# # candidate benching with this probe.
# #
# # If you fix result_hash() to use a fully-unique sort key (e.g. row_number()
# # over the candidate output) Phase B can become useful again — but read the
# # handover first to understand WHY the bench-first approach was wrong before
# # rerunning it.
# ###############################################################################
#
# Phase A: --locate
#   Drives two fresh `--fuse -f` runs in subprocesses, diffs their release parquets,
#   classifies which aggregate class owns the row drift between identical-input runs.
#   Note: COLUMN_OWNER attribution is approximate (e.g. common_name is set in
#   enrich() not dedupe_names) — treat per-class flip counts as hints, not precise blame.
#
# Phase B: --bench {vote,dedupe,higher}
#   DO NOT TRUST until result_hash() is fixed. See block above.
#
# Usage from data/ working directory:
#   uv run python importer/canopy/abtest/20_aggregate_determinism_probe.py --locate
#   uv run python importer/canopy/abtest/20_aggregate_determinism_probe.py --bench vote
#   uv run python importer/canopy/abtest/20_aggregate_determinism_probe.py --bench dedupe
#   uv run python importer/canopy/abtest/20_aggregate_determinism_probe.py --bench higher
#
# All probe artifacts land in canopy/data/temp/abtest_20/ which is gitignored.

# Standard library imports kept compact for a one-file probe.
from types import SimpleNamespace
import argparse
import hashlib
import os
import re
import shutil
import subprocess
import sys
import time

# DuckDB powers all candidate shapes and the diff queries.
import duckdb

# Force UTF-8 console output so unicode common names do not crash on Windows cp1252.
try: sys.stdout.reconfigure(encoding='utf-8', errors='replace')
except Exception: pass
try: sys.stderr.reconfigure(encoding='utf-8', errors='replace')
except Exception: pass

# Canopy proxies for runtime settings and resolved data dirs.
from importer.canopy import settings, build_settings, RELEASES_DIR, TMP_DIR
# Latest processed file discovery for in-process pre-state reconstruction.
from importer.canopy.utils.filehandlers import get_latest_processed
# Production fuse helpers reused so candidates run on the real production state.
from importer.canopy.pipeline import fuse as fuse_pipeline

# Probe artifact dir under the gitignored canopy temp tree.
PROBE_DIR = os.path.join(TMP_DIR, 'abtest_20')
# Subprocess fuse stdout/stderr capture goes here so the console stays readable.
LOG_DIR = os.path.join(PROBE_DIR, 'logs')

# Print a compact section header for log scanning.
def section(title):
	# One blank line plus boxed title for visibility.
	print(f"\n############### {title} ###############", flush=True)

# Format elapsed runtime with one decimal.
def elapsed(start):
	# Seconds since start, formatted compactly.
	return f"{time.time() - start:.1f}s"

# Resolve absolute path to canopy project root for subprocess cwd.
def canopy_root():
	# Two levels up from abtest/.
	return os.path.abspath(os.path.join(os.path.dirname(__file__), '..', '..', '..'))

# Initialize canopy settings for in-process probes.
def init_runtime():
	# Build minimal CLI args matching local non-S3 fuse.
	args = SimpleNamespace(debug=False, verbose=False, force=False, csv=False, s3=False)
	# Install runtime settings into the canopy proxy.
	settings.set_config(build_settings(args))

# Compute total byte size of the canopy temp dir, used as a spill proxy.
def temp_dir_bytes():
	# Walk temp tree and accumulate sizes; ignore probe artifacts to keep numbers comparable.
	total = 0
	# Iterate temp subtree.
	for root, _, files in os.walk(TMP_DIR):
		# Skip our own probe dir to avoid self-counting.
		if root.startswith(PROBE_DIR): continue
		# Sum file sizes for the remaining temp tree.
		for f in files:
			try: total += os.path.getsize(os.path.join(root, f))
			except OSError: pass
	# Return total bytes for delta measurement.
	return total

# Run one fresh fuse in a clean subprocess and return the path of the produced release parquet.
def run_fresh_fuse(label):
	# Ensure log dir exists for capture.
	os.makedirs(LOG_DIR, exist_ok=True)
	# Build subprocess command matching production --fuse -f invocation.
	cmd = [sys.executable, '-m', 'importer.canopy.run', '--fuse', '-f']
	# Subprocess cwd must be data/ so canopy package resolves correctly.
	cwd = os.path.join(canopy_root(), 'data')
	# Capture combined stdout/stderr for the run to a log file.
	log_path = os.path.join(LOG_DIR, f'fuse_{label}.log')
	# Announce the run with its label and target log.
	print(f"running fresh fuse {label} -> {log_path}", flush=True)
	# Time the run for operator feedback. Use this timestamp as the floor for produced parquets.
	start = time.time()
	# Add a tiny safety margin so we don't miss parquets written in the same wall-clock second as start.
	start_floor = start - 1.0
	# Open log file for write and execute fuse subprocess.
	with open(log_path, 'w', encoding='utf-8') as log:
		# Inherit env so storage/secrets resolve like production.
		result = subprocess.run(cmd, cwd=cwd, stdout=log, stderr=subprocess.STDOUT)
	# Abort the probe if the fuse failed; never silently continue.
	if result.returncode != 0: raise RuntimeError(f"fuse {label} failed; see {log_path}")
	# Find release parquets written during this run by mtime, since fuse may overwrite an existing dir.
	candidates = []
	# Iterate every release dir and pick the one whose parquet was written after the run started.
	for d in os.listdir(RELEASES_DIR):
		# Resolve dir path and skip non-directories.
		dir_path = os.path.join(RELEASES_DIR, d)
		if not os.path.isdir(dir_path): continue
		# Resolve expected release parquet path.
		parquet_path = os.path.join(dir_path, f'{d}.parquet')
		# Skip dirs without a release parquet.
		if not os.path.exists(parquet_path): continue
		# Compare parquet mtime to run start so we only consider this run's output.
		mtime = os.path.getmtime(parquet_path)
		if mtime < start_floor: continue
		# Keep candidate with its mtime for newest-wins selection.
		candidates.append((mtime, d, parquet_path))
	# Abort if the fuse run did not produce any release parquet within the run window.
	if not candidates: raise RuntimeError(f"fuse {label} produced no release parquet newer than run start; see {log_path}")
	# Pick the most recently written release parquet as this run's output.
	_, latest, release_parquet = max(candidates)
	# Copy the parquet under our probe dir so subsequent fuses cannot overwrite it.
	dest = os.path.join(PROBE_DIR, f'release_{label}.parquet')
	# Hardlink-or-copy the artifact for later diff.
	shutil.copyfile(release_parquet, dest)
	# Report runtime.
	print(f"fuse {label} produced {latest} in {elapsed(start)}", flush=True)
	# Return the stable copy path.
	return dest

# Map of accepted-row column name to the aggregate class that produces it in fuse.py.
# Used by --locate to attribute observed flips to a fix surface.
COLUMN_OWNER = {
	# Vote produces consensus columns via list_mode over per-authority pools.
	'name_consensus': 'vote.list_mode',
	'rank_consensus': 'vote.list_mode',
	'author_consensus': 'vote.list_mode',
	'year_consensus': 'vote.list_mode',
	# dedupe_names backfills mode-aggregated columns from the dupes set.
	'source': 'dedupe_names.mode',
	'ipni_id': 'dedupe_names.mode',
	'wcvp_id': 'dedupe_names.mode',
	'powo_id': 'dedupe_names.mode',
	'wfo_id': 'dedupe_names.mode',
	'col_id': 'dedupe_names.mode',
	'tropicos_id': 'dedupe_names.mode',
	'fungorum_id': 'dedupe_names.mode',
	'mycobank_id': 'dedupe_names.mode',
	'wikidata_id': 'dedupe_names.mode',
	'inaturalist_id': 'dedupe_names.mode',
	'gbif_id': 'dedupe_names.mode',
	'wikipedia_page': 'dedupe_names.mode',
	'iucn_status': 'dedupe_names.mode',
	'common_name': 'dedupe_names.mode',
	# Higher rank columns are filled by add_higher_ranks Polars mode-backfill.
	'family': 'add_higher_ranks.polars_mode',
	'order': 'add_higher_ranks.polars_mode',
	'class': 'add_higher_ranks.polars_mode',
	'phylum': 'add_higher_ranks.polars_mode',
	# Parent UUID is a downstream derivation; treat parent flips as residual after H+J restructure.
	'parent_consensus': 'consolidate_parents.cascade',
}

# Run --locate workflow: two fresh fuses, diff, classify per column.
def cmd_locate(args):
	# Ensure probe dir exists.
	os.makedirs(PROBE_DIR, exist_ok=True)
	# Drive run1 unless skipped.
	if args.skip_fuse:
		# Reuse previously copied artifacts when iterating on the diff logic.
		run1 = os.path.join(PROBE_DIR, 'release_run1.parquet')
		run2 = os.path.join(PROBE_DIR, 'release_run2.parquet')
		# Sanity check both exist before continuing.
		if not (os.path.exists(run1) and os.path.exists(run2)): raise RuntimeError('--skip-fuse but no prior run1/run2 in probe dir')
	# Fresh run pair when not skipped.
	else:
		# Always run two fresh fuses back-to-back to reflect production behavior.
		section('Phase A: two fresh --fuse -f runs')
		run1 = run_fresh_fuse('run1')
		run2 = run_fresh_fuse('run2')
	# Open in-memory DuckDB for the diff queries.
	with duckdb.connect(':memory:') as db:
		# Route DuckDB temp spill to canopy temp.
		db.execute(f"SET temp_directory='{TMP_DIR}'")
		# Cap memory so the diff query can spill if the parquets are large.
		db.execute("SET memory_limit='10GB'")
		# Register the two release parquets as views to keep SQL compact.
		db.execute(f"CREATE VIEW r1 AS SELECT * FROM read_parquet('{run1.replace(chr(92), '/')}') WHERE accepted")
		db.execute(f"CREATE VIEW r2 AS SELECT * FROM read_parquet('{run2.replace(chr(92), '/')}') WHERE accepted")
		# Section header for diff output.
		section('Phase A: accepted-name diff between two fresh fuses')
		# Macro counts: only-in-run1, only-in-run2, common.
		macro = db.execute("""
			WITH n1 AS (SELECT name_consensus, kingdom FROM r1),
			     n2 AS (SELECT name_consensus, kingdom FROM r2)
			SELECT
				(SELECT COUNT(*) FROM n1) AS r1_total,
				(SELECT COUNT(*) FROM n2) AS r2_total,
				(SELECT COUNT(*) FROM (SELECT * FROM n1 EXCEPT SELECT * FROM n2)) AS only_run1,
				(SELECT COUNT(*) FROM (SELECT * FROM n2 EXCEPT SELECT * FROM n1)) AS only_run2,
				(SELECT COUNT(*) FROM (SELECT * FROM n1 INTERSECT SELECT * FROM n2)) AS common;
		""").fetchone()
		# Print compact macro line.
		print(f"r1_total={macro[0]:,} r2_total={macro[1]:,} only_run1={macro[2]:,} only_run2={macro[3]:,} common={macro[4]:,}", flush=True)
		# Discover columns common to both releases for the per-column flip count.
		cols_r1 = {row[0] for row in db.execute("DESCRIBE r1").fetchall()}
		cols_r2 = {row[0] for row in db.execute("DESCRIBE r2").fetchall()}
		# Restrict comparison to columns the owner map knows about and both releases provide.
		compare_cols = [c for c in COLUMN_OWNER if c in cols_r1 and c in cols_r2]
		# Build join on (name_consensus, kingdom) for common rows only.
		section('Phase A: per-column flip counts on common accepted rows')
		# Quote columns when needed (DuckDB keyword for `order`).
		def q(c): return f'"{c}"' if c == 'order' else c
		# Build a single SQL that counts flips per column in one pass.
		flip_exprs = ',\n\t\t'.join([f"COUNT(*) FILTER (WHERE a.{q(c)} IS DISTINCT FROM b.{q(c)}) AS {c.replace(chr(34), '_')}_flips" for c in compare_cols])
		# Run the per-column flip count.
		flip_row = db.execute(f"""
			WITH a AS (SELECT * FROM r1),
			     b AS (SELECT * FROM r2)
			SELECT
				{flip_exprs}
			FROM a JOIN b USING (name_consensus, kingdom);
		""").fetchone()
		# Aggregate flip totals by aggregate class for the headline attribution.
		by_class = {}
		# Pair column names with flip counts in declaration order.
		flips_per_col = dict(zip(compare_cols, flip_row))
		# Accumulate totals per owner class.
		for col, flips in flips_per_col.items():
			# Look up which fuse function owns this column.
			owner = COLUMN_OWNER.get(col, 'residual')
			# Track total flips and the columns contributing to each class.
			rec = by_class.setdefault(owner, {'flips': 0, 'cols': []})
			# Sum into the class total.
			rec['flips'] += flips
			# Keep only columns that actually flipped for the report.
			if flips: rec['cols'].append((col, flips))
		# Print per-class summary.
		section('Phase A: aggregate-class attribution')
		# Sort classes by total flips descending for triage clarity.
		for owner, rec in sorted(by_class.items(), key=lambda kv: -kv[1]['flips']):
			# Show class total then top contributing columns.
			cols_str = ', '.join([f"{c}={n:,}" for c, n in sorted(rec['cols'], key=lambda x: -x[1])[:6]])
			# One compact line per class.
			print(f"  {owner:<35} flips={rec['flips']:,}  cols=[{cols_str}]", flush=True)
		# Sample 10 flipping rows for the most-affected class with both run values.
		top_class = max(by_class.items(), key=lambda kv: kv[1]['flips'])[0] if by_class else None
		# Dump representative rows when there is a top class with non-zero flips.
		if top_class and by_class[top_class]['flips']:
			# Pick the single most-flipping column to keep sample output narrow.
			top_col = max(by_class[top_class]['cols'], key=lambda x: x[1])[0]
			# Print sample header.
			section(f'Phase A: sample flipping rows for {top_class} on column {top_col}')
			# Show a small sample of differing rows side by side.
			db.sql(f"""
				WITH a AS (SELECT name_consensus, kingdom, {q(top_col)} AS run1_v FROM r1),
				     b AS (SELECT name_consensus, kingdom, {q(top_col)} AS run2_v FROM r2)
				SELECT name_consensus, kingdom, run1_v, run2_v
				FROM a JOIN b USING (name_consensus, kingdom)
				WHERE run1_v IS DISTINCT FROM run2_v
				LIMIT 10;
			""").show(max_width=200)
		# Final advisory line for next step.
		print(f"\nNext: run --bench {top_class.split('.')[0] if top_class else 'vote'} to evaluate fix candidates for the dominant class.", flush=True)

# Build the production state up to the entry point of a given aggregate class.
# Returns the in-memory DuckDB connection ready for candidates.
def build_pre_state(target):
	# Open an in-memory DuckDB connection.
	db = duckdb.connect(':memory:')
	# Route spill to canopy temp so DuckDB can fall back to scratch disk if memory is tight.
	db.execute(f"SET temp_directory='{TMP_DIR}'")
	# Cap memory so DuckDB pages to temp_directory instead of OS-killing the process.
	db.execute("SET memory_limit='10GB'")
	# Resolve latest hydrated processed inputs.
	results = get_latest_processed()
	# Run the production map phase always.
	fuse_pipeline.load_map_sources(results, db)
	# For 'vote' we need the state right before vote() runs the consensus columns.
	# initial_backbone + add_ids must happen first; vote pools come from add_ids columns.
	fuse_pipeline.initial_backbone(results, db)
	fuse_pipeline.add_ids(results, db)
	# For 'vote' we stop here; candidates probe the pool construction in basic_consensus.
	if target == 'vote': return db, results
	# For 'dedupe' we need full pre-dedupe state, so build basic consensus and hashes etc.
	fuse_pipeline.basic_consensus(results, db)
	fuse_pipeline.create_hashes(results, db)
	fuse_pipeline.load_enrich_sources(results, db)
	fuse_pipeline.enrich(results, db)
	fuse_pipeline.reduce_vernacular(results, db)
	# Pre-create higher-rank columns the same way fuse() does before dedupe runs.
	for rank in fuse_pipeline.higher_ranks:
		db.execute(f"ALTER TABLE meso ADD COLUMN IF NOT EXISTS {rank} VARCHAR;")
	# Decide acceptance before dedupe in the H+J ordering.
	fuse_pipeline.decide_acceptance(results, db)
	# 'dedupe' candidates probe the dedupe_names backfill_values block; stop before dedupe runs.
	if target == 'dedupe': return db, results
	# For 'higher' we need post-dedupe state right before add_higher_ranks Polars block.
	fuse_pipeline.dedupe_names(results, db)
	# Stop before add_higher_ranks; candidates rebuild the Polars/SQL backfill alternatives.
	if target == 'higher': return db, results
	# Unknown target: programmer error.
	raise RuntimeError(f"unknown bench target {target}")

# Hash the contents of a result table for determinism comparison across runs.
def result_hash(db, table, key_col, value_cols):
	# Concatenate (key, value) pairs sorted by key into one md5.
	value_expr = " || '|' || ".join([f"coalesce({c}::VARCHAR, '<null>')" for c in value_cols])
	# Compute md5 over the sorted concatenation.
	h = db.execute(f"""
		SELECT md5(string_agg({key_col} || ':' || ({value_expr}), chr(10) ORDER BY {key_col}))
		FROM {table};
	""").fetchone()[0]
	# Return the digest string.
	return h

# Inspect the EXPLAIN plan for cartesian/nested-loop fallback warnings.
def plan_warnings(db, sql):
	# Pull the textual plan.
	plan = db.execute(f"EXPLAIN {sql}").fetchall()
	# Flatten plan rows to a single string for regex.
	text = '\n'.join(row[1] for row in plan)
	# Surface the operators we want to flag.
	flags = []
	# Detect cross product fallback.
	if re.search(r'CROSS_PRODUCT|NESTED_LOOP_JOIN|BLOCKWISE_NL_JOIN', text, re.IGNORECASE): flags.append('NL/CROSS')
	# Return short flag string.
	return ','.join(flags) if flags else 'ok'

# Run one candidate twice and report runtime, spill delta, hash determinism.
def measure(db, label, build_sql, key_col, value_cols, current_hash=None):
	# Capture spill bytes before the candidate runs.
	pre_bytes = temp_dir_bytes()
	# First run.
	t0 = time.time()
	db.execute(build_sql)
	t1 = time.time()
	# Compute output row count.
	rows = db.execute("SELECT COUNT(*) FROM candidate_out").fetchone()[0]
	# Hash output of run1.
	h1 = result_hash(db, 'candidate_out', key_col, value_cols)
	# Spill size after run1.
	mid_bytes = temp_dir_bytes()
	# Plan inspection on the build SQL (best-effort, only printed if anomalies).
	# EXPLAIN on a CREATE OR REPLACE statement is supported by DuckDB.
	warnings = 'n/a'
	try: warnings = plan_warnings(db, build_sql)
	except Exception: pass
	# Drop and re-run for determinism check.
	db.execute("DROP TABLE candidate_out")
	t2 = time.time()
	db.execute(build_sql)
	t3 = time.time()
	# Hash output of run2.
	h2 = result_hash(db, 'candidate_out', key_col, value_cols)
	# Spill size after run2.
	post_bytes = temp_dir_bytes()
	# Determinism flag.
	deterministic = h1 == h2
	# Semantic delta vs current behavior, if a current hash was provided.
	matches_current = (h1 == current_hash) if current_hash else None
	# Spill peak proxy.
	spill_mb = max(mid_bytes, post_bytes) - pre_bytes
	# Print compact result line.
	print(
		f"  {label:<32} run1={t1-t0:5.1f}s run2={t3-t2:5.1f}s rows={rows:>9,} "
		f"spill_delta={spill_mb/1e6:6.1f}MB plan={warnings} "
		f"deterministic={'YES' if deterministic else 'NO '} "
		f"vs_current={'same' if matches_current else 'diff' if matches_current is False else 'n/a'} "
		f"h1={h1[:8]}",
		flush=True,
	)
	# Drop output table to keep memory bounded across candidates.
	db.execute("DROP TABLE candidate_out")
	# Return primary hash for cross-candidate comparison.
	return h1

# vote.list_mode candidates: stabilize per-row consensus over an authority pool list.
# We focus on name_clean because Phase A attributes ~692 flips here (author/rank/year consensus).
# Each candidate writes (id_meso, name_winner) into candidate_out.
def bench_vote(db):
	# Announce target.
	section('Phase B: bench vote.list_mode (target column: name_clean -> name_consensus)')
	# Replicate vote()'s pool construction directly so we can snapshot the pool before vote drops it.
	# Pool entries come from per-authority name_clean values joined via {authority}_id.
	# Filter mirrors production: skip names with >=4 tokens or apostrophes (which production rejects).
	# Build per-authority columns inline using a single CREATE TABLE so DuckDB can plan it columnar.
	authorities = fuse_pipeline.core_authorities
	# Subquery per authority to pull name_clean by id, applying production's quality filter.
	selects = []
	# Build one LEFT JOIN per authority to materialize name_clean_{authority}.
	join_clauses = []
	# Iterate the authorities production uses for name_clean voting.
	for auth in authorities:
		# Production rejects names with leftover ranks/quotes; mirror that filter.
		selects.append(f"CASE WHEN {auth}.name_clean IS NOT NULL AND len(string_split({auth}.name_clean,' ')) < 4 AND NOT contains({auth}.name_clean, '''') THEN {auth}.name_clean END AS name_clean_{auth}")
		# Left join the source authority on its id column.
		join_clauses.append(f"LEFT JOIN {auth} ON meso.{auth}_id = {auth}.id_raw")
	# Materialize the pool table directly without polluting the meso table.
	db.execute(f"""
		CREATE OR REPLACE TEMP TABLE vote_raw AS
			SELECT meso.rowid AS id_meso, {', '.join(selects)}
			FROM meso
			{' '.join(join_clauses)};
	""")
	# Build the pool list and keep only rows with at least one authority name.
	db.execute(f"""
		CREATE OR REPLACE TEMP TABLE vote_input AS
			SELECT id_meso,
				list_filter([{', '.join([f'name_clean_{a}' for a in authorities])}], lambda x: x IS NOT NULL) AS pool
			FROM vote_raw;
	""")
	# Drop empty pools so candidates only see real voting cases.
	db.execute("DELETE FROM vote_input WHERE pool IS NULL OR len(pool) = 0")
	# Report shape.
	shape = db.execute("SELECT COUNT(*), avg(len(pool)), max(len(pool)) FROM vote_input").fetchone()
	print(f"vote_input: rows={shape[0]:,} avg_pool_len={shape[1]:.2f} max_pool_len={shape[2]}", flush=True)
	# Candidate: current production behavior using list_mode without ORDER BY.
	current_sql = """
		CREATE OR REPLACE TEMP TABLE candidate_out AS
			SELECT id_meso, trim(list_mode(pool)) AS winner
			FROM vote_input;
	"""
	# Candidate: presort the pool list before list_mode so tied elements feed mode in stable order.
	presort_sql = """
		CREATE OR REPLACE TEMP TABLE candidate_out AS
			SELECT id_meso, trim(list_mode(list_sort(pool))) AS winner
			FROM vote_input;
	"""
	# Candidate: explicit unnest + GROUP BY + QUALIFY row_number with deterministic tie-break.
	unnest_qualify_sql = """
		CREATE OR REPLACE TEMP TABLE candidate_out AS
			WITH unnested AS (
				SELECT id_meso, unnest(pool) AS v FROM vote_input
			),
			counted AS (
				SELECT id_meso, v, COUNT(*) AS c
				FROM unnested
				WHERE v IS NOT NULL
				GROUP BY id_meso, v
			)
			SELECT id_meso, trim(v) AS winner
			FROM counted
			QUALIFY row_number() OVER (PARTITION BY id_meso ORDER BY c DESC, v ASC) = 1;
	"""
	# Candidate: mode aggregate with explicit ORDER BY clause inside the aggregate.
	mode_orderby_sql = """
		CREATE OR REPLACE TEMP TABLE candidate_out AS
			WITH unnested AS (
				SELECT id_meso, unnest(pool) AS v FROM vote_input WHERE pool IS NOT NULL
			)
			SELECT id_meso, trim(mode(v ORDER BY v)) AS winner
			FROM unnested
			WHERE v IS NOT NULL
			GROUP BY id_meso;
	"""
	# Run current first to obtain the reference hash.
	current_hash = measure(db, 'current (list_mode)', current_sql, 'id_meso', ['winner'])
	# Run remaining candidates against the same input.
	measure(db, 'presort_pool', presort_sql, 'id_meso', ['winner'], current_hash)
	measure(db, 'unnest_qualify', unnest_qualify_sql, 'id_meso', ['winner'], current_hash)
	measure(db, 'mode_orderby', mode_orderby_sql, 'id_meso', ['winner'], current_hash)

# dedupe_names.mode candidates: stabilize the backfill_values MODE block.
# Probe a representative subset of mode columns so we can compare shapes without bloating runtime.
def bench_dedupe(db):
	# Announce target.
	section('Phase B: bench dedupe_names.mode (backfill_values block)')
	# Build dupes table the same way dedupe_names does so candidates see real input.
	db.execute("""
		CREATE OR REPLACE TEMP TABLE dupes_input AS
			SELECT *,
				len(accepted_by) AS acceptance_count,
				len(considered_synonym) AS synonym_count
			FROM meso
			WHERE accepted
			  AND name_consensus IN (
			      SELECT name_consensus FROM meso WHERE accepted GROUP BY name_consensus HAVING COUNT(*) > 1
			  );
	""")
	# Print shape.
	shape = db.execute("SELECT COUNT(*), COUNT(DISTINCT name_consensus) FROM dupes_input").fetchone()
	print(f"dupes_input: rows={shape[0]:,} distinct_names={shape[1]:,}", flush=True)
	# Probe columns chosen to span class behavior: source (varchar enum), gbif_id (int), iucn_status (varchar).
	probe_cols = ['source', 'gbif_id', 'iucn_status', 'common_name', 'wikipedia_page']
	# Build SELECT lists for each candidate shape over the probe columns.
	# Current: bare mode aggregate.
	current_select = ', '.join([f'mode({c}) AS {c}' for c in probe_cols])
	# mode_orderby: mode with explicit ORDER BY by value then id_meso for stable tie-break.
	orderby_select = ', '.join([f'mode({c} ORDER BY {c}, id_meso) AS {c}' for c in probe_cols])
	# arg_max_count: precompute counts then arg_max on count with secondary value tiebreak.
	# Build via window-then-group to avoid per-column subqueries.
	# Candidate SQL templates.
	current_sql = f"""
		CREATE OR REPLACE TEMP TABLE candidate_out AS
			SELECT name_consensus, {current_select}
			FROM dupes_input
			GROUP BY name_consensus;
	"""
	orderby_sql = f"""
		CREATE OR REPLACE TEMP TABLE candidate_out AS
			SELECT name_consensus, {orderby_select}
			FROM dupes_input
			GROUP BY name_consensus;
	"""
	# qualify_winner builds one window per probe column and joins them on name_consensus.
	# This validates whether the explicit window shape is competitive on multi-column dedupe.
	qualify_ctes = []
	# Build one CTE per probe column.
	for c in probe_cols:
		qualify_ctes.append(f"""
			{c}_winner AS (
				SELECT name_consensus, {c}
				FROM (
					SELECT name_consensus, {c}, COUNT(*) AS cnt
					FROM dupes_input
					WHERE {c} IS NOT NULL
					GROUP BY name_consensus, {c}
				)
				QUALIFY row_number() OVER (PARTITION BY name_consensus ORDER BY cnt DESC, {c}::VARCHAR ASC) = 1
			)""")
	# Compose the final SELECT joining all per-column winners.
	qualify_join = '\n\t\t\t'.join([f"LEFT JOIN {c}_winner USING (name_consensus)" for c in probe_cols])
	# Build the full qualify SQL using a base of distinct names.
	qualify_sql = f"""
		CREATE OR REPLACE TEMP TABLE candidate_out AS
			WITH base AS (SELECT DISTINCT name_consensus FROM dupes_input),
			{','.join(qualify_ctes)}
			SELECT b.name_consensus, {', '.join([f'{c}_winner.{c} AS {c}' for c in probe_cols])}
			FROM base b
			{qualify_join};
	"""
	# Bench current first for reference hash.
	current_hash = measure(db, 'current (mode)', current_sql, 'name_consensus', probe_cols)
	# Bench the mode-with-orderby variant.
	measure(db, 'mode_orderby', orderby_sql, 'name_consensus', probe_cols, current_hash)
	# Bench the QUALIFY-window variant.
	measure(db, 'qualify_winner', qualify_sql, 'name_consensus', probe_cols, current_hash)

# add_higher_ranks Polars mode-backfill: bench DuckDB and Polars deterministic shapes.
# Production species/genus/family/etc columns are populated INSIDE add_higher_ranks; pre-state
# is empty. We use a previously produced release parquet as realistic input shape instead.
def bench_higher(db):
	# Announce target.
	section('Phase B: bench add_higher_ranks higher-rank backfill')
	# Locate a fresh release parquet under the probe dir from a prior --locate run.
	run1 = os.path.join(PROBE_DIR, 'release_run1.parquet')
	# Abort with guidance when no release is available.
	if not os.path.exists(run1): raise RuntimeError(f"need {run1} from --locate; run --locate first")
	# Path normalize for DuckDB on Windows.
	run1_path = run1.replace(os.sep, '/')
	# Load post-add_higher_ranks shape from the release parquet.
	db.execute(f"""
		CREATE OR REPLACE TEMP TABLE higher_input AS
			SELECT id_meso AS child_id, species, genus, family, "order", class, phylum
			FROM read_parquet('{run1_path}')
			WHERE species IS NOT NULL;
	""")
	# Print shape.
	shape = db.execute("SELECT COUNT(*), COUNT(DISTINCT species) FROM higher_input").fetchone()
	print(f"higher_input: rows={shape[0]:,} distinct_species={shape[1]:,}", flush=True)
	# Current behavior emulated in DuckDB: list_mode without tiebreak per species->genus backfill.
	# We rebuild current as DuckDB list_mode for a fair runtime baseline.
	current_sql = """
		CREATE OR REPLACE TEMP TABLE candidate_out AS
			SELECT species, list_mode(array_agg(genus)) AS genus
			FROM higher_input
			WHERE genus IS NOT NULL
			GROUP BY species;
	"""
	# mode_orderby: mode with explicit ORDER BY by genus value for stable tie-break.
	orderby_sql = """
		CREATE OR REPLACE TEMP TABLE candidate_out AS
			SELECT species, mode(genus ORDER BY genus) AS genus
			FROM higher_input
			WHERE genus IS NOT NULL
			GROUP BY species;
	"""
	# qualify_winner: explicit window with deterministic tie-break.
	qualify_sql = """
		CREATE OR REPLACE TEMP TABLE candidate_out AS
			SELECT species, genus
			FROM (
				SELECT species, genus, COUNT(*) AS cnt
				FROM higher_input
				WHERE genus IS NOT NULL
				GROUP BY species, genus
			)
			QUALIFY row_number() OVER (PARTITION BY species ORDER BY cnt DESC, genus ASC) = 1;
	"""
	# Bench current as the reference hash.
	current_hash = measure(db, 'current (list_mode)', current_sql, 'species', ['genus'])
	# Bench mode with explicit ORDER BY.
	measure(db, 'mode_orderby', orderby_sql, 'species', ['genus'], current_hash)
	# Bench window-based winner.
	measure(db, 'qualify_winner', qualify_sql, 'species', ['genus'], current_hash)

# Drive a --bench run for one of {vote, dedupe, higher}.
def cmd_bench(args):
	# Ensure probe dir exists for any artifacts.
	os.makedirs(PROBE_DIR, exist_ok=True)
	# Initialize canopy runtime so production helpers can build state.
	init_runtime()
	# Higher-rank bench reads a release parquet directly; no heavy pre-state needed.
	if args.bench == 'higher':
		# Open a fresh DuckDB connection with the same spill/memory envelope as build_pre_state.
		db = duckdb.connect(':memory:')
		db.execute(f"SET temp_directory='{TMP_DIR}'")
		db.execute("SET memory_limit='10GB'")
	# All other targets need the production pre-state.
	else:
		# Build the production pre-state for the chosen target.
		section(f'Phase B: building production pre-state for {args.bench}')
		# Time the build for operator feedback.
		start = time.time()
		db, _ = build_pre_state(args.bench)
		# Report build runtime.
		print(f"pre-state ready in {elapsed(start)}", flush=True)
	# Dispatch to the right bench function.
	try:
		# Route to the matching candidate suite.
		if args.bench == 'vote': bench_vote(db)
		elif args.bench == 'dedupe': bench_dedupe(db)
		elif args.bench == 'higher': bench_higher(db)
		# Unknown target.
		else: raise RuntimeError(f"unknown --bench {args.bench}")
	# Always close DB to release memory.
	finally: db.close()

# Top-level argument parser and dispatcher.
def main():
	# Build CLI parser.
	parser = argparse.ArgumentParser(description='Aggregate determinism probe and AB bench')
	# One of --locate or --bench must be picked.
	mode = parser.add_mutually_exclusive_group(required=True)
	# Phase A driver.
	mode.add_argument('--locate', action='store_true', help='Drive two fresh fuses, diff, classify aggregate-class drift')
	# Phase B driver targets one aggregate class.
	mode.add_argument('--bench', choices=['vote', 'dedupe', 'higher'], help='Bench candidate shapes for one aggregate class')
	# Allow reusing previously copied run1/run2 artifacts when iterating on diff logic.
	parser.add_argument('--skip-fuse', action='store_true', help='Reuse prior run1/run2 parquets in probe dir instead of running fresh fuses')
	# Parse args.
	args = parser.parse_args()
	# Dispatch to the chosen mode.
	if args.locate: cmd_locate(args)
	# Bench dispatch.
	elif args.bench: cmd_bench(args)

# Run as script.
if __name__ == '__main__':
	# Enter main routine.
	main()
