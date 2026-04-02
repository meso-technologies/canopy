#
#		NCBI Data Importer
#		Oracle DB dump would be about weekly, but Checklistbank CoL dataset is about quarterly
#
#		Highlights: Used to look up proteins
# Internal
from .. import SRC_DIR, TMP_DIR, settings

# File handling
import zipfile
from ..utils.filehandlers import fetch

# DB
import duckdb
from ..utils.queries import strip_author_from_name, name_cleanup, find_hybrids, build_rank_and_status, validate, write_to_disc, language_mappings

source = {
    "name": "ncbi",
    "url": "https://hosted-datasets.gbif.org/datasets/ncbi.zip",
	"citation": '<a href="https://www.ncbi.nlm.nih.gov/" class="medium">NCBI</a>, Schoch CL, et al., National Library of Medicine (US) National Center for Biotechnology Information, Bethesda (MD). NCBI Taxonomy: a comprehensive update on curation, resources and tools. Database (Oxford). YYYY-MM-DD. <a href="https://pubmed.ncbi.nlm.nih.gov/32761142/" class="medium">DOI:10.1093/database/baaa062</a>'
}

# Main function called as asyncio Task from run.py
async def update_ncbi(session):
	print(f"IMPORT : ############### Starting NCBI Update  ###############")
	update_available = await fetch(session, source)
	# See if we have an update and if yes process it
	if (update_available or settings.FORCE) and not settings.DOWNLOAD_ONLY: process_ncbi(source)
	# Return the source dict containing processing outcomes
	return source

# Process a fresh sourcefile
def process_ncbi(source: dict):
	print(f"IMPORT : Starting to process { source['latest_download'] }...") 
	# Load zipfile and duckdb
	with zipfile.ZipFile(f"{SRC_DIR}/{source['latest_download']}", 'r') as zip, duckdb.connect(':memory:') as db:
		# Route DuckDB spill files to canopy temp directory
		db.execute(f"SET temp_directory = '{TMP_DIR}'")
		# Load the initial csv file
		taxa_csv = db.read_csv(zip.open('taxa.txt'), parallel=True, header=False, delimiter="\t", names=['id', 'parent', 'synonym', 'rank', 'scientificName', 'comment'])
		# Createinitial
		db.execute(f"""
			CREATE TABLE { source['name'] } 
				AS (SELECT 
					-- ~2.7M rows after synonym filter. Broad but noisy supplemental source
					CAST(id AS UINTEGER) AS id_raw, 
					scientificName AS name_raw,		
			 		lower(scientificName) AS name_clean,
					-- 25 clean ranks: species ~2.2M, genus ~112k, subspecies ~30k
					rank as rank_raw,
					CAST(NULL AS VARCHAR) AS rank_clean,
					CAST(parent AS UINTEGER) AS parent_raw,
					-- synonym rows have '-s' suffix in id, filtered out
					FROM taxa_csv AS taxa WHERE id NOT LIKE '%-s%'
				);
		""")
		# Find hybrids
		find_hybrids(db,source)
		# Generic cleanup
		name_cleanup(db,source)
		# Build ranks
		build_rank_and_status(db,source)
		# Fetch all vernacular names below
		ncbi_vernacular(zip,db)
		# Final validation
		validate(db,source)
		# Write to disc
		write_to_disc(db,source)

# Add English vernacular names from vernacular.txt, only ~1% of taxa get names
def ncbi_vernacular(zip: zipfile.ZipFile, db: duckdb.DuckDBPyConnection):
	# Load vernacular.txt (headerless TSV) and aggregate English names per taxon
	vernacular_csv = db.read_csv(zip.open('vernacular.txt'), parallel=True, header=False, null_padding=True, delimiter="\t", names=['id','vernacularName'])
	db.execute(f"""
		ALTER TABLE ncbi ADD COLUMN IF NOT EXISTS vernacular VARCHAR[];
		-- all NCBI vernacular names are English, hardcode 'en' prefix
		UPDATE ncbi n SET vernacular = (
			SELECT array_agg('en' || ':' || trim(lower(v.vernacularName)))
			FROM vernacular_csv v WHERE n.id_raw = v.id GROUP BY v.id
		);
	""")
	if settings.VERBOSE: db.sql(f"""SELECT id_raw, name_clean, vernacular FROM ncbi WHERE vernacular IS NOT NULL ORDER BY len(vernacular) DESC;""").show(max_rows=100)
