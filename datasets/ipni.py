#
# 		International Plant Name Index Importer 
#		The Core backbone of where get our ICN initial plant names from
#

# Internal
from .. import SRC_DIR, TMP_DIR, settings

# File handling
import zipfile
from ..utils.filehandlers import fetch

# DB
import duckdb
from ..utils.queries import strip_rank_from_name, name_cleanup, find_hybrids, build_rank_and_status, validate, write_to_disc

source = {
    "name": "ipni",
    "url": "https://hosted-datasets.gbif.org/datasets/ipni.zip",
	"citation": '<a href="https://www.ipni.org/" class="medium">IPNI</a>, The Royal Botanic Gardens, Kew, Harvard University Herbaria & Libraries, & Australian National Botanic Gardens. (YYYY). International Plant Names Index (IPNI). Version YYYY-MM-DD. <a href="https://www.ipni.org/" class="medium">https://www.ipni.org/</a>'
}

# Main function called as asyncio Task from run.py
async def update_ipni(session):
	print(f"IMPORT : ############### Starting IPNI Update  ###############")
	update_available = await fetch(session, source)
	# See if we have an update and if yes process it
	if (update_available or settings.FORCE) and not settings.DOWNLOAD_ONLY: process_ipni(source)
	# Return the source dict containing processing outcomes
	return source

# Process a fresh source file
def process_ipni(source: dict):
	print(f"IMPORT : Starting to process { source['latest_download'] }...")  
	# Resolve local source path (already ensured by fetch in S3 mode)
	source_path = source.get('local_path') or f"{SRC_DIR}/{source['latest_download']}"
	# Load zipfile and duckdb
	with zipfile.ZipFile(source_path, 'r') as zip, duckdb.connect(':memory:') as db:
		# Route DuckDB spill files to canopy temp directory
		db.execute(f"SET temp_directory = '{TMP_DIR}'")
		# Load the initial tsv files
		name_tsv = db.read_csv(zip.open('Name.tsv'),parallel=True)
		# "col:ID" is always equal to "col:nameID"
		taxa_tsv = db.read_csv(zip.open('Taxon.tsv'),parallel=True)
		# We have up to 7 relations per entry
		relation_tsv = db.read_csv(zip.open('NameRelation.tsv'),parallel=True)
		# Pulbication
		sources_tsv = db.read_csv(zip.open('Reference.tsv'),parallel=True)
		# Create merged table by selecting all fields we're interested and create placeholders for further data
		db.execute(f"""
			CREATE TABLE ipni AS SELECT 
				-- Name.tsv col:ID, ~1.79M rows. Core plant nomenclatural authority
				n."col:ID" as id_raw,
				n."col:scientificName" as name_raw,
				lower(n."col:scientificName") AS name_clean,
				n."col:authorship" as author_raw,
				n."col:rank" as rank_raw,
				CAST(NULL AS VARCHAR) AS rank_clean,
				-- col:publishedInYear with two known data errors corrected
				CASE 
					WHEN n."col:publishedInYear" = '71997' THEN 1997
					WHEN n."col:publishedInYear" = '187' THEN 1876
					ELSE CAST(n."col:publishedInYear" AS USMALLINT)
				END AS year,
				-- Taxon.tsv family, joined by col:nameID
				lower(t."col:family") as family
			FROM name_tsv n
			LEFT JOIN taxa_tsv t ON n."col:ID" = t."col:nameID"
		""")
		# Add synonyms from NameRelation.tsv, ~23% of rows have at least one synonym
		db.execute("""
			ALTER TABLE ipni ADD COLUMN synonyms STRUCT(id VARCHAR, type VARCHAR)[];
			UPDATE ipni SET synonyms = (
				SELECT list(struct_pack(
					id := "col:relatedNameID",
					type := "col:type"
				))
				FROM relation_tsv WHERE "col:nameID" = ipni.id_raw
			);
		""")
		# Add publication and BHL IDs from Reference.tsv via col:remarks patterns
		db.execute("""
			ALTER TABLE ipni ADD COLUMN publication VARCHAR;
			ALTER TABLE ipni ADD COLUMN bhl_title UINTEGER;
			ALTER TABLE ipni ADD COLUMN bhl_page UINTEGER;
			UPDATE ipni SET 
				-- Reference.tsv col:title, max 120 chars
			 	publication = substring(s."col:title", 1, 120),
				-- BHL title/page IDs extracted from col:remarks [bhl_title_id:NNN] pattern
				bhl_title = NULLIF(REGEXP_EXTRACT(lower(s."col:remarks"), '\\[bhl_title_id:(\\d+)\\]', 1),''),
				bhl_page = NULLIF(REGEXP_EXTRACT(lower(s."col:remarks"), '\\[bhl_page_id:(\\d+)\\]', 1),'')
			FROM name_tsv n JOIN sources_tsv s ON n."col:referenceID" = s."col:ID" WHERE n."col:ID" = ipni.id_raw;
		""")
		print(f"IMPORT : Added { db.execute('SELECT COUNT(publication) FROM ' + source['name']).fetchone()[0] } publications and { db.execute('SELECT COUNT(bhl_page) FROM ' + source['name']).fetchone()[0] } BHL pages to { source['name'] }")		
		# Log
		print(f"IMPORT : Loaded { db.execute('SELECT COUNT(*) FROM ' + source['name']).fetchone()[0] } plant names from { source['name'] }")
		# IPNI-specific data quality fix: stray author fragments embedded in name_clean
		db.execute(f"UPDATE ipni SET name_clean = replace(replace(name_clean,' sieber ex dc. ',' '),' dc. ',' ')")
		# Find hybrids
		find_hybrids(db,source)
		# Strip ranks from name
		strip_rank_from_name(db,source)
		# Generic cleanup
		name_cleanup(db,source)
		# Build ranks
		build_rank_and_status(db,source)
		# Final validation
		validate(db,source)
		# Write to disc
		write_to_disc(db,source)
