# Litmus stage overview:
# - Runs lightweight regression sentinels against a completed release.
# - Writes compact pass/fail summary into release manifest as:
#   litmus = { passed, failed, issues }.
# - Keeps checks intentionally small and explicit so failures are actionable.
#
# Strict data-source warning:
# - Litmus queries must use ONLY the final main release parquet exposed as `meso`.
# - Do not read geo/source/processed/temp/intermediate artifacts here.
# - If a condition cannot be validated from `meso`, it does not belong in litmus.

# Load canopy logger for litmus stage progress and failures
from ..utils.log import mesologger
# Load canopy release directory constant
from .. import RELEASES_DIR
# Load release manifest helper for standalone litmus runs
from ..utils.filehandlers import get_latest_release
# Load storage proxy for local/S3 manifest IO
from ..utils.s3 import storage
# Load filesystem path helper for release artifact paths
import os
# Load DuckDB for parquet-backed SQL checks
import duckdb


# Keep litmus checks as simple tuples for easy maintenance.
# Tuple shape: (key, query, expect, issue_message)
checks = [
	# Prevent silent catastrophic release shrinkage.
	# This catches broad acceptance regressions before we inspect individual taxa.
	# Query returns one row only when the floor is satisfied.
	(
		'accepted_taxa_floor',
		"""
		-- Require a minimum accepted taxa count in final meso output.
		SELECT 1
		FROM (SELECT COUNT(*) AS accepted_taxa FROM meso WHERE accepted)
		WHERE accepted_taxa >= 700000
		""",
		'any',
		'Accepted taxa count below floor 700000'
	),
	# Guard critical detail-volume floors used by downstream taxonomy UX surfaces.
	# Litmus is meso-only and reads observations from embedded geo scalar totals.
	# Query returns one row only when all three floors are satisfied.
	(
		'taxon_detail_floor',
		"""
		-- Require minimum detail volumes from final meso output only.
		SELECT 1
		FROM (
			SELECT
				(SELECT COALESCE(SUM(CAST(json_extract_string(geo, '$.observations') AS BIGINT)), 0) FROM meso WHERE accepted AND geo IS NOT NULL) AS total_occurrences,
				(SELECT COALESCE(SUM(len(names)), 0) FROM (SELECT unnest(map_values(vernacular)) AS names FROM meso WHERE accepted AND vernacular IS NOT NULL) v) AS total_vernacular,
				(SELECT COUNT(*) FROM meso WHERE wikipedia_page IS NOT NULL AND trim(wikipedia_page) <> '') AS total_abstracts
		)
		WHERE total_occurrences > 520000000 AND total_vernacular > 1000000 AND total_abstracts > 100000
		""",
		'any',
		'Taxon detail floor failed occurrences vernacular or abstracts below threshold'
	),
	# Lock two concrete NCBI regressions that previously broke in production.
	# We intentionally compare exact expected IDs, not just non-null presence.
	# Query returns mismatches or missing accepted rows so expect is none.
	(
		'ncbi_sentinels_exact_ids',
		"""
		-- Return sentinel rows that are missing or mapped to the wrong NCBI ID.
		WITH expected(name_consensus, expected_ncbi_id) AS (VALUES ('solanum melongena', 4111), ('fragaria ananassa', 3747))
		SELECT e.name_consensus, e.expected_ncbi_id, m.ncbi_id AS got_ncbi_id
		FROM expected e
		LEFT JOIN meso m ON m.name_consensus = e.name_consensus AND m.accepted
		WHERE m.name_consensus IS NULL OR m.ncbi_id IS NULL OR m.ncbi_id <> e.expected_ncbi_id
		""",
		'none',
		'NCBI sentinel IDs mismatch or missing for solanum melongena or fragaria ananassa'
	),
	# Guard historically missing accepted taxa so they do not silently disappear again.
	# This keeps a tight sentinel list from resolved notes without expanding broad scope.
	# We pass when both taxa are present as accepted rows.
	(
		'historically_missing_acceptance',
		"""
		-- salvia rosmarinus and vaccinium vitisidaea must remain accepted.
		WITH expected(name_consensus) AS (VALUES ('salvia rosmarinus'), ('vaccinium vitisidaea'))
		SELECT e.name_consensus
		FROM expected e
		LEFT JOIN meso m ON m.accepted AND m.name_consensus = e.name_consensus
		WHERE m.name_consensus IS NULL
		""",
		'none',
		'Historically missing taxa no longer present as accepted rows salvia rosmarinus or vaccinium vitisidaea'
	),
	# Guard high-visibility Wikipedia linkage for common species pages.
	# Query by scientific names only and require an accepted row with a page slug.
	# This catches regressions where name linkage exists but page output is blank.
	(
		'wikipedia_sentinels_present_for_common_species',
		"""
		-- Return sentinel taxa that lack accepted Wikipedia page linkage.
		WITH expected(name_consensus) AS (VALUES ('malus domestica'), ('leontopodium nivale'), ('vaccinium myrtillus'))
		SELECT e.name_consensus
		FROM expected e
		LEFT JOIN meso m ON m.name_consensus = e.name_consensus AND m.accepted
		WHERE m.name_consensus IS NULL OR m.wikipedia_page IS NULL OR TRIM(m.wikipedia_page) = ''
		""",
		'none',
		'Wikipedia page sentinel missing for malus domestica leontopodium nivale or vaccinium myrtillus'
	),
	# Lock parent-canonical sentinels that previously drifted between duplicate genus rows.
	# These exact accepted genus rows have strong multi-authority support and high child counts.
	# We pass when all rows exist as accepted GENUS parents with enough children still attached.
	(
		'canonical_genus_parent_sentinels',
		"""
		-- Return stable parent genera that are missing, unaccepted, wrong rank, or lost most children.
		WITH expected(name_consensus, author_consensus, year_consensus, min_children) AS (VALUES ('epidendrum','L.',1763,1500),('erica','L.',1753,750),('sphagnum','L.',1753,250),('bazzania','Gray',1821,200),('wedelia','Jacq.',1760,140))
		SELECT e.name_consensus, e.author_consensus, e.year_consensus, e.min_children, m.accepted, m.rank_consensus, m.child_count
		FROM expected e
		LEFT JOIN meso m ON m.name_consensus = e.name_consensus AND m.author_consensus = e.author_consensus AND m.year_consensus = e.year_consensus
		WHERE m.id_meso IS NULL OR NOT m.accepted OR m.rank_consensus != 'GENUS' OR COALESCE(m.child_count, 0) < e.min_children
		""",
		'none',
		'Canonical genus parent sentinel missing unaccepted wrong rank or child count below floor'
	),
	# Lock Wikidata exact-name assignment sentinels from ambiguous external-ID cases.
	# These stable accepted species have exact Wikidata name matches and six accepted authority signals.
	# We pass when the fused row keeps the expected Meso UUID and cross-source IDs.
	(
		'wikidata_exact_name_id_stability',
		"""
		-- Return stable accepted species whose Wikidata/source IDs drifted away from exact-name assignments.
		WITH expected(name_consensus, id_meso, wikidata_id, gbif_id, col_id, inaturalist_id, wcvp_id, powo_id, wfo_id) AS (VALUES ('averrhoa carambola','39d0a0ff-4b49-5f9e-ad22-2b0030ee3d03'::UUID,'Q159447',9407103,'K327',146977,2666746,'371870-1','wfo-0000557405'),('comarum palustre','b2f0c3e6-94ae-56c7-8c2c-bfd7c3684ea7'::UUID,'Q18198979',3020532,'XDR2',62213,2943891,'63773-2','wfo-0000985679'),('fragaria ananassa','a543c1c8-9afb-5e04-b571-479ddbeefe02'::UUID,'Q13158',3029912,'6JK32',55366,2948609,'30117681-2','wfo-0001005541'),('neottia ovata','37d7eea7-4649-53c5-b87b-5ff49e28457c'::UUID,'Q15502088',2816250,'CDR5G',341154,134271,'645363-1','wfo-0000250598'),('tetragonia tetragonoides','6c589398-8031-552b-a790-18e91e691536'::UUID,'Q278283',5554574,'7C2YJ',418653,2445492,'364795-1','wfo-0000414976'))
		SELECT e.name_consensus, e.id_meso, m.wikidata_id, m.gbif_id, m.col_id, m.inaturalist_id, m.wcvp_id, m.powo_id, m.wfo_id
		FROM expected e
		LEFT JOIN meso m ON m.id_meso = e.id_meso
		WHERE m.id_meso IS NULL OR NOT m.accepted OR m.name_consensus != e.name_consensus OR m.wikidata_id != e.wikidata_id OR m.gbif_id != e.gbif_id OR m.col_id != e.col_id OR m.inaturalist_id != e.inaturalist_id OR m.wcvp_id != e.wcvp_id OR m.powo_id != e.powo_id OR m.wfo_id != e.wfo_id
		""",
		'none',
		'Wikidata exact-name ID sentinel missing unaccepted or cross-source IDs changed'
	),
	# Lock known-good GBIF sentinels with exact name+id and embedded geo payload.
	# This asserts accepted row identity and verifies geo embedding produced map-ready data.
	# We pass when query returns zero regressions.
	(
		'gbif_name_id_and_geo_sentinels',
		"""
		-- atropa belladonna caucasica (3802658), cynodon nlemfuensis nlemfuensis (7226806), dudleya abramsii murina (4199721), puccinia striiformis striiformis (7187198), prunus armeniaca (7818643), claviceps purpurea (9378152), hibiscus sabdariffa (3152582), mentha piperita (8707933), citrus aurantium (8077391), daucus carota sativus (6550056)
		WITH expected(name_consensus, gbif_id) AS (VALUES ('atropa belladonna caucasica',3802658),('cynodon nlemfuensis nlemfuensis',7226806),('dudleya abramsii murina',4199721),('puccinia striiformis striiformis',7187198),('prunus armeniaca',7818643),('claviceps purpurea',9378152),('hibiscus sabdariffa',3152582),('mentha piperita',8707933),('citrus aurantium',8077391),('daucus carota sativus',6550056))
		SELECT e.name_consensus, e.gbif_id
		FROM expected e LEFT JOIN meso m ON m.accepted AND m.name_consensus = e.name_consensus AND m.gbif_id = e.gbif_id
		WHERE m.gbif_id IS NULL OR m.geo IS NULL OR ((COALESCE(json_array_length(json_extract(m.geo, '$.habitat')), 0) = 0) AND (COALESCE(json_array_length(json_extract(m.geo, '$.centroids')), 0) = 0))
		""",
		'none',
		'GBIF sentinel mismatch missing accepted name id pair or missing embedded geo habitat centroids'
	),
]

# Run litmus checks against a release parquet and persist results into release manifest.
def run(release=None):
	# Resolve release manifest when caller did not pass one.
	if not release: release = get_latest_release()
	# Abort gracefully when no release manifest is available.
	if not release:
		# Log missing release context.
		mesologger.warning('Litmus skipped because no release manifest was found')
		# Return no payload for caller.
		return None
	# Read release version for artifact path resolution.
	version = release.get('version')
	# Abort when manifest is malformed and lacks version.
	if not version:
		# Log malformed release payload.
		mesologger.error('Litmus skipped because release manifest has no version')
		# Return no payload for caller.
		return None
	# Build release parquet path from canonical release naming.
	release_parquet = os.path.join(RELEASES_DIR, version, f'{version}.parquet')
	# Verify release parquet exists before running checks.
	if not storage.exists(release_parquet):
		# Log missing release parquet.
		mesologger.error(f'Litmus skipped because release parquet was not found: {release_parquet}')
		# Return no payload for caller.
		return None
	# Build DuckDB-readable URL/path for parquet access.
	parquet_url = storage.parquet_url(release_parquet)
	# Announce litmus stage and target release.
	mesologger.info(f'############### Running Litmus for {version} ###############')
	# Keep failures as manifest issue strings.
	issues = []
	# Run checks in one short-lived in-memory DuckDB connection.
	with duckdb.connect(':memory:') as db:
		# Configure DuckDB S3 access when storage backend is object storage.
		if storage.is_s3(): storage.configure_duckdb(db)
		# Expose release parquet as meso view for tuple queries.
		db.execute(f"CREATE VIEW meso AS SELECT * FROM read_parquet('{parquet_url}')")
		# Iterate all configured litmus checks.
		for key, query, expect, issue in checks:
			# Execute check query and collect returned rows.
			rows = db.execute(query).fetchall()
			# Evaluate pass/fail from expected row-shape semantics.
			passed = (len(rows) > 0) if expect == 'any' else (len(rows) == 0)
			# Log successful checks for operator visibility.
			if passed:
				# Log litmus pass with stable check key.
				mesologger.info(f'LITMUS OK {key}')
			# Otherwise capture one issue message and log failure detail.
			else:
				# Store issue string for manifest summary.
				issues.append(issue)
				# Log failure with expected semantics and row count.
				mesologger.warning(f'LITMUS FAIL {key} expected {expect} got {len(rows)} rows')
	# Build compact litmus summary payload for manifest consumers.
	litmus = {
		# Count checks that passed.
		'passed': len(checks) - len(issues),
		# Count checks that failed.
		'failed': len(issues),
		# Keep issue list for quick operator triage.
		'issues': issues,
	}
	# Attach litmus summary to release manifest payload.
	release['litmus'] = litmus
	# Persist updated release manifest to storage backend.
	storage.write_json(os.path.join(RELEASES_DIR, version, 'manifest.json'), release)
	# Log final litmus stage summary.
	mesologger.info(f"Litmus complete for {version}: {litmus['passed']} passed, {litmus['failed']} failed")
	# Return litmus payload to caller for optional control flow.
	return litmus
