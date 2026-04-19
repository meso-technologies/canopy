#
#		NCBI Data Importer
#
#		Direct from the canonical NCBI FTP, refreshed daily. We previously
#		pulled this via hosted-datasets.gbif.org which GBIF flipped to ColDP
#		in April 2026. The NCBI dump is stable and strictly richer than GBIF's
#		former export (author strings, merged-taxid redirects, cleaner vernacular).
#
#		Source format is BCP-like: field terminator is \t|\t, row terminator
#		is \t|\n, so every row has a trailing \t| on the last column that we
#		rtrim off before typed casts.
#
#		Files consumed from new_taxdump.zip:
#		  nodes.dmp    taxonomy nodes with parent and rank
#		  names.dmp    scientific names, authority, synonyms, common names by class
#		  merged.dmp   obsolete taxid -> current taxid redirects (~97k)
#
#		Other files in the same zip (typematerial, citations, host, rankedlineage,
#		images) are available but not surfaced until downstream consumers exist.
#
# Internal
from ..utils.log import mesologger
from .. import SRC_DIR, TMP_DIR, settings

# File handling
import zipfile
from ..utils.filehandlers import fetch

# DB
import duckdb
from ..utils.queries import name_cleanup, find_hybrids, build_rank_and_status, validate, write_to_disc

source = {
	# Canonical source name, unchanged so existing processed parquets and diff.py keep working
	"name": "ncbi",
	# NCBI refreshes this dump daily, served with an MD5 sidecar for integrity checking
	"url": "https://ftp.ncbi.nlm.nih.gov/pub/taxonomy/new_taxdump/new_taxdump.zip",
	"citation": '<a href="https://www.ncbi.nlm.nih.gov/" class="medium">NCBI</a>, Schoch CL, et al., National Library of Medicine (US) National Center for Biotechnology Information, Bethesda (MD). NCBI Taxonomy: a comprehensive update on curation, resources and tools. Database (Oxford). YYYY-MM-DD. <a href="https://pubmed.ncbi.nlm.nih.gov/32761142/" class="medium">DOI:10.1093/database/baaa062</a>'
}

# nodes.dmp has 18 columns in new_taxdump; we only consume the first 5, rest are read as VARCHAR
# so the sniffer cannot trip on a future NCBI schema expansion
NODE_COLS = {
	'taxid':'UINTEGER', 'parent':'UINTEGER', 'rank':'VARCHAR', 'embl':'VARCHAR',
	'division_id':'UTINYINT', 'c5':'VARCHAR', 'c6':'VARCHAR', 'c7':'VARCHAR',
	'c8':'VARCHAR', 'c9':'VARCHAR', 'c10':'VARCHAR', 'c11':'VARCHAR',
	'c12':'VARCHAR', 'c13':'VARCHAR', 'c14':'VARCHAR', 'c15':'VARCHAR',
	'c16':'VARCHAR', 'c17':'VARCHAR',
}
# names.dmp is a stable 4-column shape
NAME_COLS = {'taxid':'UINTEGER','name':'VARCHAR','unique_name':'VARCHAR','name_class':'VARCHAR'}
# merged.dmp is two columns; new_taxid arrives with trailing \t| that we rtrim below
MERGED_COLS = {'old_taxid':'UINTEGER','new_taxid_raw':'VARCHAR'}

# Shared read_csv kwargs for BCP-style .dmp files.
# Note: duckdb python binding uses 'delimiter'/'quotechar' while the SQL function uses 'delim'/'quote'.
# These go through db.read_csv() so we stick with the python spellings.
DMP_OPTS = dict(delimiter='\t|\t', header=False, quotechar='', parallel=True)

# SQL snippet that trims the trailing "\t|" left by the row terminator on the last column
TRAIL = "chr(9) || '|'"

# Main function called as asyncio Task from run.py
async def update_ncbi(session):
	mesologger.info(f"############### Starting NCBI Update  ###############")
	# Fetch source file
	update_available = await fetch(session, source)
	# See if we have an update and if yes process it
	if (update_available or settings.FORCE) and not settings.DOWNLOAD_ONLY: process_ncbi(source)
	# Return the source dict containing processing outcomes
	return source

# Process a fresh sourcefile
def process_ncbi(source: dict):
	mesologger.info(f"Starting to process { source['latest_download'] }...")
	# Resolve local source path (already ensured by fetch in S3 mode)
	source_path = source.get('local_path') or f"{SRC_DIR}/{source['latest_download']}"
	# Load zipfile and duckdb
	with zipfile.ZipFile(source_path, 'r') as zip, duckdb.connect(':memory:') as db:
		# Route DuckDB spill files to canopy temp directory
		db.execute(f"SET temp_directory = '{TMP_DIR}'")
		# Load the three dmp files we consume and register each as a named table so the
		# downstream views survive across function boundaries. DuckDB's replacement-scan
		# lookup of Python locals only works within the same call frame, whereas
		# db.register puts the relation into the catalog for the whole connection lifetime.
		db.register('nodes_raw',  db.read_csv(zip.open('nodes.dmp'),  columns=NODE_COLS,   **DMP_OPTS))
		db.register('names_raw',  db.read_csv(zip.open('names.dmp'),  columns=NAME_COLS,   **DMP_OPTS))
		db.register('merged_raw', db.read_csv(zip.open('merged.dmp'), columns=MERGED_COLS, **DMP_OPTS))
		# Materialise trimmed rows into real tables so subsequent joins do not rescan the zip,
		# and so downstream functions can see them without depending on Python-side scope.
		db.execute(f"""
			-- Only the columns we actually consume from nodes.dmp
			CREATE TEMP TABLE nodes AS SELECT taxid, parent, rank FROM nodes_raw;
			-- Trim name_class trailer so we can do equality checks on clean values
			CREATE TEMP TABLE names AS SELECT taxid, name, rtrim(name_class, {TRAIL}) AS name_class FROM names_raw;
			-- Trim and cast merged redirects
			CREATE TEMP TABLE merged AS SELECT old_taxid, CAST(rtrim(new_taxid_raw, {TRAIL}) AS UINTEGER) AS new_taxid FROM merged_raw;
		""")
		# Build core ncbi table: one row per accepted taxid with scientific name + author
		build_core_table(db)
		# Shared hybrid detection sets hybrid + hybridpos and strips the × sign from name_clean
		find_hybrids(db, source)
		# Shared name cleanup for punctuation, whitespace, ascii folding
		name_cleanup(db, source)
		# Shared rank normalization. We do not emit status_raw because we only retain scientific-name rows
		build_rank_and_status(db, source)
		# Attach English vernacular names restricted to true vernacular classes
		ncbi_vernacular(db)
		# Attach authoritative merged-taxid redirects so fuse.polish() can rewrite obsolete ncbi_ids deterministically
		ncbi_merged(db)
		# Final validation
		validate(db, source)
		# Write to disc
		write_to_disc(db, source)

# Build one row per accepted taxid. Authority rows in names.dmp are joined in to populate author_raw
# so NCBI can contribute to the author consensus vote in fuse.basic_consensus
def build_core_table(db: duckdb.DuckDBPyConnection):
	# Build the core table via a CTE that pivots scientific-name + authority rows onto one row per taxid
	db.execute("""
		CREATE TABLE ncbi AS
		WITH sci AS (
			-- The scientific-name row is the authoritative single name per taxid
			SELECT taxid, name AS sci_name FROM names WHERE name_class = 'scientific name'
		), auth AS (
			-- Pick the authority row that starts with the exact scientific name. This filters out
			-- authority rows that belong to synonyms living on the same taxid (e.g. Arabidopsis
			-- thaliana has two authorities, only "Arabidopsis thaliana (L.) Heynh., 1842" matches
			-- the scientific name; "Arabis thaliana L., 1753" belongs to the Arabis thaliana synonym).
			SELECT n.taxid, any_value(n.name) AS authority_name
			FROM names n JOIN sci s ON n.taxid = s.taxid
			WHERE n.name_class = 'authority'
			AND starts_with(n.name, s.sci_name)
			AND length(n.name) > length(s.sci_name)
			GROUP BY n.taxid
		)
		SELECT
			-- Core identity
			n.taxid AS id_raw,
			s.sci_name AS name_raw,
			lower(s.sci_name) AS name_clean,
			-- Rank straight from nodes.dmp, shared build_rank_and_status normalises it into rank_clean
			n.rank AS rank_raw,
			CAST(NULL AS VARCHAR) AS rank_clean,
			n.parent AS parent_raw,
			-- Author text, derived by trimming the scientific-name prefix off the authority row.
			-- Result looks like "(L.) Heynh., 1842" or "L., 1753" or NULL when no authority row.
			NULLIF(trim(substring(a.authority_name, length(s.sci_name) + 1)), '') AS author_raw
		FROM nodes n
		JOIN sci s ON s.taxid = n.taxid
		LEFT JOIN auth a ON a.taxid = n.taxid;
	""")
	# Log row count and author coverage
	rows = db.execute("SELECT COUNT(*) FROM ncbi").fetchone()[0]
	with_auth = db.execute("SELECT COUNT(*) FROM ncbi WHERE author_raw IS NOT NULL").fetchone()[0]
	mesologger.info(f"Built ncbi core with {rows:,} rows ({with_auth:,} with author_raw populated)")

# Attach English vernacular names. Restricted to the two classes that actually represent vernaculars:
# 'common name' (free-form) and 'genbank common name' (the curated NCBI canonical label). Other classes
# in names.dmp like 'equivalent name', 'includes', 'blast name' are taxonomic aliases or informal groups,
# not vernaculars, and would poison the vernacular pool if included.
def ncbi_vernacular(db: duckdb.DuckDBPyConnection):
	db.execute("""
		ALTER TABLE ncbi ADD COLUMN IF NOT EXISTS vernacular VARCHAR[];
		WITH vern AS (
			-- Aggregate unique lowercased vernaculars per taxid, prefixed 'en:' for reduce_vernacular
			SELECT taxid, array_agg(DISTINCT 'en:' || trim(lower(name))) AS names
			FROM names
			WHERE name_class IN ('common name', 'genbank common name')
			GROUP BY taxid
		)
		UPDATE ncbi n SET vernacular = v.names FROM vern v WHERE n.id_raw = v.taxid;
	""")
	taxa = db.execute("SELECT COUNT(*) FROM ncbi WHERE vernacular IS NOT NULL").fetchone()[0]
	strings = db.execute("SELECT SUM(len(vernacular)) FROM ncbi WHERE vernacular IS NOT NULL").fetchone()[0] or 0
	mesologger.info(f"Attached vernaculars to {taxa:,} taxa ({strings:,} strings total)")

# Attach authoritative merged-taxid history. NCBI publishes ~97k historical merges where old taxid X
# was collapsed into current taxid Y. fuse.polish() uses this to rewrite obsolete ncbi_ids that entered
# our pipeline via Wikidata before the merge happened, without having to rely on name-match heuristics.
def ncbi_merged(db: duckdb.DuckDBPyConnection):
	db.execute("""
		ALTER TABLE ncbi ADD COLUMN IF NOT EXISTS merged_from UINTEGER[];
		WITH m AS (SELECT new_taxid, array_agg(old_taxid) AS olds FROM merged GROUP BY new_taxid)
		UPDATE ncbi n SET merged_from = m.olds FROM m WHERE n.id_raw = m.new_taxid;
	""")
	hits = db.execute("SELECT COUNT(*) FROM ncbi WHERE merged_from IS NOT NULL").fetchone()[0]
	mesologger.info(f"Attached merged-taxid redirects to {hits:,} accepted rows")
