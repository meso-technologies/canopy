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

# Acceptance authorities publish taxonomic acceptance flags we scope diffs against
ACCEPTANCE = {'powo','wcvp','wfo','col','gbif','iucn','inaturalist'}
# Registries publish names or cross-references without taxonomic acceptance semantics
REGISTRY   = {'ipni','tropicos','fungorum','mycobank','ncbi','wikidata'}
# Bibliographic enrichment source has non-unique id_raw so diff is meaningless there
EXCLUDED   = {'bhl'}

# Tracked content columns per source drive the "changed" detection
# Year changes are tracked where available since they represent real upstream metadata improvements
CHANGE_COLS = {
	# Core plant backbones with author, rank and publication year
	'powo':        ['name_clean','rank_clean','author_raw','year'],
	'wcvp':        ['name_clean','rank_clean','author_raw','year'],
	'wfo':         ['name_clean','rank_clean','author_raw','year'],
	'col':         ['name_clean','rank_clean','author_raw','year'],
	# Registries with author, rank and publication year
	'ipni':        ['name_clean','rank_clean','author_raw','year'],
	'fungorum':    ['name_clean','rank_clean','author_raw','year'],
	'mycobank':    ['name_clean','rank_clean','author_raw','year'],
	# Cross-kingdom backbone with publication year
	'gbif':        ['name_clean','rank_clean','author_raw','year'],
	# NCBI carries author from the names.dmp authority rows but no publication year
	'ncbi':        ['name_clean','rank_clean','author_raw'],
	# Specimen registry has no rank column but tracks year
	'tropicos':    ['name_clean','author_raw','year'],
	# IUCN conservation status flips are the key signal alongside basic name/rank drift (no year column)
	'iucn':        ['name_clean','rank_clean','author_raw','iucn_status'],
	# iNat carries acceptance and rank only (no year column)
	'inaturalist': ['name_clean','rank_clean'],
	# Wikidata tracks cross-reference flips, flags and publication year
	'wikidata':    ['name_clean','rank_clean','year','ipni_id','powo_id','wfo_id','col_id','gbif_id','ncbi_id','iucn_id','edible','toxic','medicinal'],
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
			# Classify the source so diff kind and scope expression can be chosen
			kind = 'acceptance' if name in ACCEPTANCE else 'registry'
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
			# Build per-source diff block and sidecar entries
			diff_block, sidecar = _diff_one_source(db, name, kind, cur_path, cur_parquet, prev_path, prev_parquet)
			# Attach scalar diff block to source entry in manifest
			source['diff'] = diff_block
			# Preserve per-source sample arrays for combined sidecar write
			sidecar_sources[name] = sidecar
			# Emit grep-friendly summary log for operator triage
			_log_source_summary(name, diff_block)
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
	# Inject top-level diff filename so build_detail surfaces it automatically via summary.artifacts
	manifest['diff'] = sidecar_name
	# Write manifest back with injected diff blocks and filename
	storage.write_json(manifest_path, manifest)
	mesologger.info(f"Updated {manifest_path} with per-source diff blocks")

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

# Dispatch one source to first-release, unchanged-file, or full-diff code paths
def _diff_one_source(db, name, kind, cur_path, cur_parquet, prev_path, prev_parquet):
	# First-release path when no baseline or baseline lacks this source
	if not prev_path: return _diff_first_release(db, name, kind, cur_path)
	# Unchanged-file shortcut when baseline and current processed artifact filenames match
	if cur_parquet == prev_parquet: return _diff_unchanged(kind, prev_parquet)
	# Full diff path runs parametric FOJ query across both parquets
	return _diff_full(db, name, kind, cur_path, prev_path, prev_parquet)

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

# Emit all-new diff when no baseline release exists for this source
def _diff_first_release(db, name, kind, cur_path):
	# Build scope expression for first-release counting under the source's kind
	scope_expr = 'accepted' if kind == 'acceptance' else 'TRUE'
	# Count accepted/present rows in current parquet for scope metadata
	cur_count = db.execute(f"""
		SELECT COUNT(*) FROM read_parquet($cur) WHERE COALESCE({scope_expr}, FALSE)
	""", {'cur': cur_path}).fetchone()[0]
	# Respect sample cap so a 1.78M-row first IPNI release does not produce a 140 MB sidecar
	cap = SAMPLE_CAP
	# Collect capped sample entries mirroring the full-diff entry shape
	samples = _collect_first_release_samples(db, name, kind, cur_path, cap)
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

# Run the parametric full-outer-join diff query for one source
def _diff_full(db, name, kind, cur_path, prev_path, prev_parquet):
	# Resolve scope expression once so it participates in both CTEs and samples
	scope_expr = 'accepted' if kind == 'acceptance' else 'TRUE'
	# Get tracked content columns for this source
	cols = CHANGE_COLS[name]
	# Build optional rank selector since tropicos lacks rank_clean entirely
	rank_select = 'rank_clean' if 'rank_clean' in cols else "NULL::VARCHAR AS rank_clean"
	# Build per-column DISTINCT FROM expressions for "changed" detection
	distinct_exprs = [f"c.{col} IS DISTINCT FROM p.{col}" for col in cols]
	# Combine DISTINCT FROM expressions into one OR chain
	changed_expr = ' OR '.join(distinct_exprs) if distinct_exprs else 'FALSE'
	# Build per-column fields array expressions for the sidecar changed entries
	field_cases = []
	# One CASE per tracked column to populate the fields list conditionally
	for col in cols:
		field_cases.append(f"CASE WHEN c.{col} IS DISTINCT FROM p.{col} THEN '{col}' END")
	# Combine into a list-building expression that drops NULLs
	fields_expr = f"list_filter([{', '.join(field_cases)}], x -> x IS NOT NULL)" if field_cases else "[]"
	# Project current and previous columns into one joined table for downstream tagging
	select_cols = ', '.join(cols)
	# Build the parametric FOJ query that tags each row new/changed/deleted in one pass
	query = f"""
		WITH c AS (
			SELECT id_raw::VARCHAR AS id_raw, COALESCE({scope_expr}, FALSE) AS acc,
				   name_clean, {rank_select}, {select_cols}
			FROM read_parquet($cur)
		),
		p AS (
			SELECT id_raw::VARCHAR AS id_raw, COALESCE({scope_expr}, FALSE) AS acc,
				   name_clean, {rank_select}, {select_cols}
			FROM read_parquet($prev)
		),
		tagged AS (
			SELECT COALESCE(c.id_raw, p.id_raw) AS id,
				CASE
					WHEN (p.id_raw IS NULL OR NOT p.acc) AND c.acc       THEN 'new'
					WHEN p.acc AND (c.id_raw IS NULL OR NOT c.acc)        THEN 'deleted'
					WHEN p.acc AND c.acc AND ({changed_expr})             THEN 'changed'
				END AS category,
				COALESCE(c.name_clean, p.name_clean) AS name,
				COALESCE(c.rank_clean, p.rank_clean) AS rank,
				-- Fields list is only meaningful on changed entries where both sides exist
				CASE WHEN p.acc AND c.acc AND ({changed_expr}) THEN {fields_expr} ELSE [] END AS diff_fields
			FROM c FULL OUTER JOIN p USING (id_raw)
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
	rows = db.execute(query, {'cur': cur_path, 'prev': prev_path}).fetchall()
	# Collect scope totals for denominator and suspicion computation
	scope = db.execute(f"""
		SELECT
			(SELECT COUNT(*) FROM read_parquet($prev) WHERE COALESCE({scope_expr}, FALSE)) AS prev_scope,
			(SELECT COUNT(*) FROM read_parquet($cur)  WHERE COALESCE({scope_expr}, FALSE)) AS cur_scope
	""", {'cur': cur_path, 'prev': prev_path}).fetchone()
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

# Collect capped first-release samples mirroring full-diff entry shape
def _collect_first_release_samples(db, name, kind, cur_path, cap):
	# Resolve scope expression so we only sample rows the source considers in-scope
	scope_expr = 'accepted' if kind == 'acceptance' else 'TRUE'
	# Tropicos lacks rank_clean so project plain NULL to keep rank projection uniform
	cols = CHANGE_COLS[name]
	rank_expr = 'rank_clean' if 'rank_clean' in cols else 'NULL'
	# Run bounded SELECT with LIMIT to avoid loading 1.78M rows into Python for first IPNI run
	rows = db.execute(f"""
		SELECT id_raw::VARCHAR AS id, name_clean AS name, {rank_expr} AS rank
		FROM read_parquet($cur)
		WHERE COALESCE({scope_expr}, FALSE)
		LIMIT {cap}
	""", {'cur': cur_path}).fetchall()
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
