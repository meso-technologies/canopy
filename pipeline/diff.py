#
#	Per-source diff between the current release and a chosen baseline release
#	Emits scalar new/changed/deleted counts into manifest.json and a combined
#	diff.json sidecar with capped sample entries. Baseline defaults to previous
#	on-disk release; wrapper can override via --diff-against to point at the
#	currently-published release instead (so staging-candidate diffs compare
#	against production, not another staging candidate).
#

# Load canopy logger for structured stage output
from ..utils.log import mesologger
# Load standard library helpers for filesystem paths
import os
# Load DuckDB for parametric full-outer-join diff queries
import duckdb
# Load canopy release dir constant for release path construction
from .. import RELEASES_DIR
# Load shared storage proxy for local/S3 transparent JSON and parquet IO
from ..utils.s3 import storage
# Load release resolution helpers shared with other stages
from ..utils.filehandlers import get_latest_release, get_previous_release, get_release

# Bibliographic enrichment source has non-unique id_raw so diff is meaningless there
EXCLUDED = {'bhl'}

# Keep one profile map for source-level diff semantics and tracked changed fields
DIFF_PROFILES = {
	# Core plant backbones with author, rank and publication year
	'powo':        {'kind': 'authority', 'cols': ['name_clean','rank_clean','author_raw','year']},
	'wcvp':        {'kind': 'authority', 'cols': ['name_clean','rank_clean','author_raw','year']},
	'wfo':         {'kind': 'authority', 'cols': ['name_clean','rank_clean','author_raw','year']},
	'col':         {'kind': 'authority', 'cols': ['name_clean','rank_clean','author_raw','year']},
	# Registries with author, rank and publication year
	'ipni':        {'kind': 'registry', 'cols': ['name_clean','rank_clean','author_raw','year']},
	'fungorum':    {'kind': 'registry', 'cols': ['name_clean','rank_clean','author_raw','year']},
	'mycobank':    {'kind': 'registry', 'cols': ['name_clean','rank_clean','author_raw','year']},
	# Cross-kingdom backbone with publication year
	'gbif':        {'kind': 'authority', 'cols': ['name_clean','rank_clean','author_raw','year']},
	# NCBI currently carries author but no publication year
	'ncbi':        {'kind': 'registry', 'cols': ['name_clean','rank_clean','author_raw']},
	# Tropicos now carries rank_clean in processed output
	'tropicos':    {'kind': 'registry', 'cols': ['name_clean','rank_clean','author_raw','year']},
	# IUCN conservation status flips are high-signal change events
	'iucn':        {'kind': 'authority', 'cols': ['name_clean','rank_clean','author_raw','iucn_status']},
	# iNat carries authority + cleaned names/ranks only (no year)
	'inaturalist': {'kind': 'authority', 'cols': ['name_clean','rank_clean']},
	# Wikidata tracks cross-reference flips, flags and publication year
	'wikidata':    {'kind': 'registry', 'cols': ['name_clean','rank_clean','year','ipni_id','powo_id','wfo_id','col_id','gbif_id','ncbi_id','iucn_id','edible','toxic','medicinal']},
}

# Keep one explicit profile for fused meso accepted-taxa diff reporting
MESO_DIFF_PROFILE = {
	'kind': 'authority',
	'id_col': 'id_meso',
	'name_col': 'name',
	'rank_col': 'rank',
	'cols': ['name', 'rank', 'author', 'year', 'parent_consensus', 'iucn_status'],
}

# Cap sample entry count per category to keep sidecar lean even on wide churn
SAMPLE_CAP = 2000
# Relative delta ratio that marks a source as suspicious (broken upstream dump)
SUSPICIOUS_RATIO = 0.10
# Tighter sample cap applied to suspicious-flagged sources
SUSPICIOUS_SAMPLE_CAP = 200

# Entry point called from run.py when --diff flag is active or during full default flow
def run(release=None, diff_against=None):
	# Announce start so long canopy runs have a visible stage marker
	mesologger.info(f"############### Producing Release Diff ###############")
	# Fall back to latest on-disk release when caller did not provide one
	if not release: release = get_latest_release()
	# Abort when no release is available at all
	if not release:
		mesologger.warning("No release found on disk, skipping diff stage")
		return
	# Extract current release version for downstream path construction
	version = release.get('version')
	# Resolve baseline release honoring explicit --diff-against override when valid
	previous = _resolve_baseline(version, diff_against)
	# Resolve canopy release dir for writing diff.json and reading per-source parquets
	release_dir = os.path.join(RELEASES_DIR, version)
	# Use read_json to load the freshly-written manifest so we can inject diff blocks
	manifest_path = os.path.join(release_dir, 'manifest.json')
	# Read current manifest from active storage backend
	manifest = storage.read_json(manifest_path)
	# Open in-memory DuckDB connection for parametric diff queries
	with duckdb.connect(':memory:') as db:
		# Configure DuckDB S3 settings when release artifacts live in object storage
		if storage.is_s3(): storage.configure_duckdb(db)
		# Accumulate per-source sidecar payloads keyed by source name
		sidecar_sources = {}
		# Iterate through manifest source entries in stable order
		for name, source in manifest.get('sources', {}).items():
			# Skip bibliographic source since its id_raw is non-unique
			if name in EXCLUDED: continue
			# Resolve explicit profile for source-level diff semantics
			profile = DIFF_PROFILES.get(name)
			# Skip unknown sources explicitly so new feeds require profile declaration
			if not profile:
				mesologger.info(f"[diff] {name} skipped no_diff_profile")
				continue
			# Resolve current parquet path from manifest
			cur_parquet = source.get('latest_processed')
			# Skip sources without a processed artifact so we do not invent diffs
			if not cur_parquet:
				mesologger.info(f"[diff] {name} skipped no_processed_artifact")
				continue
			# Build absolute current parquet path inside release dir
			cur_path = os.path.join(release_dir, cur_parquet)
			# Resolve previous parquet path from baseline manifest when it exists
			prev_parquet, prev_path = _resolve_prev_parquet(previous, name)
			# Build per-source diff block and sidecar entries through one shared profile path
			diff_block, sidecar = _diff_profile(db, profile, cur_path, cur_parquet, prev_path, prev_parquet)
			# Attach scalar diff block to source entry in manifest
			source['diff'] = diff_block
			# Preserve per-source sample arrays for combined sidecar write
			sidecar_sources[name] = sidecar
			# Emit grep-friendly summary log for operator triage
			_log_source_summary(name, diff_block)
		# Resolve current and previous fused meso parquet paths for dataset-level diff
		cur_meso_parquet = f"{version}.parquet"
		cur_meso_path = os.path.join(release_dir, cur_meso_parquet)
		prev_version = previous.get('version') if previous else None
		prev_meso_parquet = f"{prev_version}.parquet" if prev_version else None
		prev_meso_path = os.path.join(RELEASES_DIR, prev_version, prev_meso_parquet) if prev_version else None
		# Build meso accepted-scope diff and append it into sidecar sources
		meso_block, meso_sidecar = _diff_profile(db, MESO_DIFF_PROFILE, cur_meso_path, cur_meso_parquet, prev_meso_path, prev_meso_parquet)
		sidecar_sources['meso'] = meso_sidecar
		# Emit grep-friendly summary log for meso-level dataset diff
		_log_source_summary('meso', meso_block)
		# Build combined sidecar payload with versioning metadata for consumers
		sidecar_payload = {
			'schema': 1,
			'from_version': previous.get('version') if previous else None,
			'to_version':   version,
			'sources':      sidecar_sources,
		}
		# Canonical sidecar filename embeds release version so one file per release
		sidecar_name = f'diff.{version}.json'
		# Write combined sidecar into canopy release dir for distill pickup
		storage.write_json(os.path.join(release_dir, sidecar_name), sidecar_payload)
		mesologger.info(f"Wrote diff sidecar {sidecar_name}")
	# Inject top-level meso diff summary and sidecar pointer
	meso_block['sidecar'] = sidecar_name
	manifest['diff'] = meso_block
	# Write manifest back with injected diff blocks and filename
	storage.write_json(manifest_path, manifest)
	mesologger.info(f"Updated {manifest_path} with per-source diff blocks and meso diff summary")

# Resolve baseline release honoring explicit override with graceful fallback to on-disk previous
def _resolve_baseline(current_version, diff_against):
	# Honor explicit --diff-against when caller provided one
	if diff_against:
		# Try loading the named baseline from canopy release dir
		previous = get_release(diff_against)
		# Return immediately when explicit baseline resolves cleanly
		if previous: return previous
		# Warn and fall back so operators know the diff is not against the requested baseline
		mesologger.warning(f"Requested baseline {diff_against} not found on disk, falling back to previous on-disk release")
	# Fall back to most recent predecessor for standalone runs and missing explicit baselines
	return get_previous_release(current_version)

# Resolve previous parquet path and filename for one source from baseline manifest
def _resolve_prev_parquet(previous, name):
	# Return a None pair when no baseline is available at all
	if not previous: return None, None
	# Look up matching source entry in baseline manifest
	prev_source = previous.get('sources', {}).get(name, {})
	# Extract previous processed filename to detect unchanged-file case
	prev_parquet = prev_source.get('latest_processed')
	# Return a None pair when baseline lacks processed artifact for this source
	if not prev_parquet: return None, None
	# Build absolute previous parquet path inside baseline release dir
	prev_path = os.path.join(RELEASES_DIR, previous.get('version'), prev_parquet)
	# Return both filename and absolute path for diff query consumption
	return prev_parquet, prev_path

# Dispatch one configured profile to first-release, unchanged-file, or full-diff code paths
def _diff_profile(db, profile, cur_path, cur_parquet, prev_path, prev_parquet):
	# Resolve kind from profile (authority or registry)
	kind = profile['kind']
	# Resolve optional id/name/rank overrides with source defaults
	id_col = profile.get('id_col', 'id_raw')
	name_col = profile.get('name_col', 'name_clean')
	rank_col = profile.get('rank_col', 'rank_clean')
	# Resolve tracked column list from profile
	cols = profile.get('cols', [])
	# First-release path when no baseline or predecessor parquet is missing
	if not prev_path or not storage.exists(prev_path): return _diff_first_release(db, kind, cur_path, id_col=id_col, name_col=name_col, rank_col=rank_col)
	# Unchanged-file shortcut when baseline and current processed artifact filenames match
	if cur_parquet == prev_parquet: return _diff_unchanged(kind, prev_parquet)
	# Full diff path runs parametric FOJ query across both parquets
	return _diff_full(db, kind, cur_path, prev_path, prev_parquet, id_col=id_col, name_col=name_col, rank_col=rank_col, cols=cols)

# Emit zero-counts block when source parquet is identical between releases
def _diff_unchanged(kind, prev_parquet):
	# Scalar block describes the skipped state without inventing scope numbers
	diff_block = {
		'from':       prev_parquet,
		'kind':       kind,
		'note':       'source file unchanged',
		'counts':     {'new': 0, 'changed': 0, 'deleted': 0},
		'scope':      {'prev': 0, 'cur': 0},
		'suspicious': False,
		'truncated':  False,
	}
	# Sidecar carries empty arrays so consumer code paths remain uniform
	sidecar = {'new': [], 'changed': [], 'deleted': []}
	# Return tuple matching the _diff_full contract
	return diff_block, sidecar

# Emit all-new diff when no baseline release exists for this profile
def _diff_first_release(db, kind, cur_path, id_col='id_raw', name_col='name_clean', rank_col='rank_clean'):
	# Build scope expression for first-release counting under the source's kind
	scope_expr = 'accepted' if kind == 'authority' else 'TRUE'
	# Resolve backend-aware parquet URL for DuckDB (local path or s3:// URL)
	cur_parquet_url = _duck_path(cur_path)
	# Count accepted/present rows in current parquet for scope metadata
	cur_count = db.execute(f"""
		SELECT COUNT(*) FROM read_parquet($cur) WHERE COALESCE({scope_expr}, FALSE)
	""", {'cur': cur_parquet_url}).fetchone()[0]
	# Respect sample cap so a 1.78M-row first IPNI release does not produce a 140 MB sidecar
	cap = SAMPLE_CAP
	# Collect capped sample entries mirroring the full-diff entry shape
	samples = _collect_first_release_samples(db, cur_path, cap, scope_expr, id_col=id_col, name_col=name_col, rank_col=rank_col)
	# Flag truncation when current scope exceeds sample cap
	truncated = cur_count > cap
	# Build scalar block with first-release semantics
	diff_block = {
		'from':       None,
		'kind':       kind,
		'counts':     {'new': cur_count, 'changed': 0, 'deleted': 0},
		'scope':      {'prev': 0, 'cur': cur_count},
		'suspicious': False,
		'truncated':  truncated,
	}
	# Sidecar carries new entries only since there is nothing to compare against
	sidecar = {'new': samples, 'changed': [], 'deleted': []}
	# Return tuple matching the _diff_full contract
	return diff_block, sidecar

# Run the parametric full-outer-join diff query for one profile
def _diff_full(db, kind, cur_path, prev_path, prev_parquet, id_col='id_raw', name_col='name_clean', rank_col='rank_clean', cols=None):
	# Resolve scope expression once so it participates in both CTEs and samples
	scope_expr = 'accepted' if kind == 'authority' else 'TRUE'
	# Resolve tracked content columns from explicit profile list
	tracked_cols = cols if cols is not None else []
	# Keep only tracked columns that exist on both current and previous parquets
	tracked_cols = _common_columns(db, cur_path, prev_path, tracked_cols)
	# Resolve backend-aware parquet URLs for DuckDB reads (local path or s3:// URL)
	cur_parquet_url = _duck_path(cur_path)
	prev_parquet_url = _duck_path(prev_path)
	# Build id/name/rank projections with type-safe string coercion
	id_expr = f"{id_col}::VARCHAR"
	name_expr = f"{name_col}" if _column_exists(db, cur_path, name_col) and _column_exists(db, prev_path, name_col) else 'NULL::VARCHAR'
	rank_expr = f"{rank_col}" if _column_exists(db, cur_path, rank_col) and _column_exists(db, prev_path, rank_col) else 'NULL::VARCHAR'
	# Build per-column DISTINCT FROM expressions for "changed" detection
	distinct_exprs = [f"c.{col} IS DISTINCT FROM p.{col}" for col in tracked_cols]
	# Combine DISTINCT FROM expressions into one OR chain
	changed_expr = ' OR '.join(distinct_exprs) if distinct_exprs else 'FALSE'
	# Build per-column fields array expressions for the sidecar changed entries
	field_cases = []
	# One CASE per tracked column to populate the fields list conditionally
	for col in tracked_cols:
		field_cases.append(f"CASE WHEN c.{col} IS DISTINCT FROM p.{col} THEN '{col}' END")
	# Combine into a list-building expression that drops NULLs
	fields_expr = f"list_filter([{', '.join(field_cases)}], x -> x IS NOT NULL)" if field_cases else "[]"
	# Project tracked columns into one joined table for downstream tagging
	tracked_select = ', ' + ', '.join(tracked_cols) if tracked_cols else ''
	# Build the parametric FOJ query that tags each row new/changed/deleted in one pass
	query = f"""
		WITH c AS (
			SELECT {id_expr} AS id_key, COALESCE({scope_expr}, FALSE) AS acc,
				   {name_expr} AS name_value, {rank_expr} AS rank_value{tracked_select}
			FROM read_parquet($cur)
		),
		p AS (
			SELECT {id_expr} AS id_key, COALESCE({scope_expr}, FALSE) AS acc,
				   {name_expr} AS name_value, {rank_expr} AS rank_value{tracked_select}
			FROM read_parquet($prev)
		),
		tagged AS (
			SELECT COALESCE(c.id_key, p.id_key) AS id,
				CASE
					WHEN (p.id_key IS NULL OR NOT p.acc) AND c.acc       THEN 'new'
					WHEN p.acc AND (c.id_key IS NULL OR NOT c.acc)        THEN 'deleted'
					WHEN p.acc AND c.acc AND ({changed_expr})             THEN 'changed'
				END AS category,
				COALESCE(c.name_value, p.name_value) AS name,
				COALESCE(c.rank_value, p.rank_value) AS rank,
				-- Fields list is only meaningful on changed entries where both sides exist
				CASE WHEN p.acc AND c.acc AND ({changed_expr}) THEN {fields_expr} ELSE [] END AS diff_fields
			FROM c FULL OUTER JOIN p USING (id_key)
		)
		SELECT
			category,
			COUNT(*) AS cnt,
			list(struct_pack(id := id, name := name, rank := rank, fields := diff_fields))[1:{SAMPLE_CAP}] AS samples
		FROM tagged
		WHERE category IS NOT NULL
		GROUP BY category
	"""
	# Execute diff in one pass against both parquets
	rows = db.execute(query, {'cur': cur_parquet_url, 'prev': prev_parquet_url}).fetchall()
	# Collect scope totals for denominator and suspicion computation
	scope = db.execute(f"""
		SELECT
			(SELECT COUNT(*) FROM read_parquet($prev) WHERE COALESCE({scope_expr}, FALSE)) AS prev_scope,
			(SELECT COUNT(*) FROM read_parquet($cur)  WHERE COALESCE({scope_expr}, FALSE)) AS cur_scope
	""", {'cur': cur_parquet_url, 'prev': prev_parquet_url}).fetchone()
	# Build category-keyed dict from the tagged result rows
	by_cat = {category: (cnt, samples) for category, cnt, samples in rows}
	# Extract per-category counts with zero defaults for missing categories
	counts = {
		'new':     by_cat.get('new',     (0, []))[0],
		'changed': by_cat.get('changed', (0, []))[0],
		'deleted': by_cat.get('deleted', (0, []))[0],
	}
	# Compute relative delta ratio for suspicion flagging
	prev_scope, cur_scope = int(scope[0]), int(scope[1])
	# Use prev scope as denominator, fall back to 1 for first-ever releases to avoid div-by-zero
	denom = max(prev_scope, 1)
	# Largest absolute delta across categories drives suspicion detection
	max_delta = max(counts['new'], counts['changed'], counts['deleted'])
	# Source is suspicious when the largest delta exceeds the configured ratio threshold
	suspicious = (max_delta / denom) > SUSPICIOUS_RATIO
	# Tighter cap applied to suspicious sources prevents runaway sample arrays
	cap = SUSPICIOUS_SAMPLE_CAP if suspicious else SAMPLE_CAP
	# Extract samples for each category honoring the applicable cap
	new_samples     = _cap_samples(by_cat.get('new',     (0, []))[1], cap)
	changed_samples = _cap_samples(by_cat.get('changed', (0, []))[1], cap)
	deleted_samples = _cap_samples(by_cat.get('deleted', (0, []))[1], cap)
	# Flag truncation when any category would have exceeded the applicable cap
	truncated = (
		counts['new']     > cap or
		counts['changed'] > cap or
		counts['deleted'] > cap
	)
	# Build scalar diff block for manifest injection
	diff_block = {
		'from':       prev_parquet,
		'kind':       kind,
		'counts':     counts,
		'scope':      {'prev': prev_scope, 'cur': cur_scope},
		'suspicious': suspicious,
		'truncated':  truncated,
	}
	# Build sidecar slice for combined diff.json write
	sidecar = {'new': new_samples, 'changed': changed_samples, 'deleted': deleted_samples}
	# Return both halves of the result for caller assembly
	return diff_block, sidecar

# Check whether a parquet file contains a specific column name

def _column_exists(db, path, column):
	# Resolve backend-aware parquet URL for DuckDB describe query
	parquet_url = _duck_path(path)
	# Query DuckDB describe output for one parquet and count matching columns
	count = db.execute("SELECT COUNT(*) FROM (DESCRIBE SELECT * FROM read_parquet($path)) d WHERE d.column_name = $column", {'path': parquet_url, 'column': column}).fetchone()[0]
	# Return boolean column existence marker
	return bool(count)

# Keep only candidate columns that exist on both parquets

def _common_columns(db, cur_path, prev_path, candidates):
	# Return candidates present on both current and previous release parquets
	return [col for col in candidates if _column_exists(db, cur_path, col) and _column_exists(db, prev_path, col)]

# Collect capped first-release samples mirroring full-diff entry shape

def _collect_first_release_samples(db, cur_path, cap, scope_expr, id_col='id_raw', name_col='name_clean', rank_col='rank_clean'):
	# Resolve backend-aware parquet URL for DuckDB reads
	cur_parquet_url = _duck_path(cur_path)
	# Build optional rank expression when rank column is absent on the current parquet
	rank_expr = rank_col if _column_exists(db, cur_path, rank_col) else 'NULL'
	# Build optional name expression when name column is absent on the current parquet
	name_expr = name_col if _column_exists(db, cur_path, name_col) else 'NULL'
	# Run bounded SELECT with LIMIT to avoid loading massive first-release datasets into Python
	rows = db.execute(f"""
		SELECT {id_col}::VARCHAR AS id, {name_expr} AS name, {rank_expr} AS rank
		FROM read_parquet($cur)
		WHERE COALESCE({scope_expr}, FALSE)
		LIMIT {cap}
	""", {'cur': cur_parquet_url}).fetchall()
	# Build entry dicts dropping rank when NULL for consistency with _normalize_entry
	return [_normalize_entry({'id': r[0], 'name': r[1], 'rank': r[2]}) for r in rows]

# Slice sample arrays to the applicable cap and normalize DuckDB struct rows to dicts
def _cap_samples(samples, cap):
	# DuckDB returns struct rows as dicts already; slice to cap for safety
	capped = samples[:cap] if samples else []
	# Drop the fields key on entries where it is empty to keep JSON lean for new/deleted categories
	return [_normalize_entry(entry) for entry in capped]

# Drop empty fields list and coerce None rank to absent key for cleaner JSON
def _normalize_entry(entry):
	# Start with id/name since both are always present
	out = {'id': entry.get('id'), 'name': entry.get('name')}
	# Include rank when non-null for UI triage
	if entry.get('rank') is not None: out['rank'] = entry['rank']
	# Include fields only when changed-detection produced at least one match
	fields = entry.get('fields')
	if fields: out['fields'] = list(fields)
	# Return cleaned entry dict ready for JSON serialization
	return out

# Resolve one parquet path into a DuckDB-readable URL for the active backend
def _duck_path(path):
	# Map local filesystem paths to themselves and S3-backed paths to s3:// URLs
	return storage.parquet_url(path)

# Emit one grep-friendly summary line per source for incident triage
def _log_source_summary(name, block):
	# Distinct log shape for skipped sources so operators can filter quickly
	if block.get('note') == 'source file unchanged':
		mesologger.info(f"[diff] {name} skipped source_file_unchanged")
		return
	# Build parts list so we can compose the log line in one format call
	parts = [
		f"prev={block['scope']['prev']}",
		f"cur={block['scope']['cur']}",
		f"new={block['counts']['new']}",
		f"changed={block['counts']['changed']}",
		f"deleted={block['counts']['deleted']}",
		f"suspicious={'true' if block['suspicious'] else 'false'}",
	]
	# Append truncated marker only when it applies so normal lines stay short
	if block['truncated']: parts.append('truncated=true')
	# Emit single-line summary matching the queries.py style
	mesologger.info(f"[diff] {name} " + ' '.join(parts))
