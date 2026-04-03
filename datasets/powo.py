#		
#		Plants of the World Online
#		One of our 3 status acceptance sources, as well as augmenting phylum, class etc in IPNI
#
#		POWO uses WCVP as the names backbone which is also used by GBIF, CoL and WFO and many others, 
# 		though they may be using older version so it is essential to check which version of WCVP they use. 
# 		The names and distribution data are updated every Monday from WCVP data harvested the previous Wednesday. 
# 		The maps are generated from the level 3 TDWG geographical codes in the WCVP database and are also refreshed 
# 		every Monday on POWO. 
#
#		TODO: Find out what subject is and why it differs from taxonomicStatus
#		TODO: Extract full lifeform/climate from dynamicProperties beyond boolean annual/perennial
#
# Internal
from .. import SRC_DIR, TMP_DIR, settings

# File handling
import zipfile
from ..utils.filehandlers import fetch

# DB
import duckdb
from ..utils.queries import strip_rank_from_name, name_cleanup, find_hybrids, build_rank_and_status, validate, write_to_disc, publication_filter


source = {
    "name": "powo",
    "url": "https://storage.googleapis.com/powop-content/backbone/powoNames.zip",
	"citation": '<a href="https://powo.science.kew.org/" class="medium">Plants of the World Online</a>, POWO. Plants of the World Online (Version YYYY-MM-DD). Facilitated by the Royal Botanic Gardens, Kew. <a href="https://powo.science.kew.org/" class="medium">https://powo.science.kew.org/</a>'
}

# Main function called as asyncio Task from run.py
async def update_powo(session):
	print(f"IMPORT : ############### Starting Plants of the World Online Update  ###############")
	update_available = await fetch(session, source)
	# See if we have an update and if yes process it
	if (update_available or settings.FORCE) and not settings.DOWNLOAD_ONLY: process_powo(source)
	# Return the source dict containing processing outcomes
	return source
    
# Process a fresh source file
def process_powo(source: dict):
	print(f"IMPORT : Starting to process { source['latest_download'] }...") 
	# Resolve local source path (already ensured by fetch in S3 mode)
	source_path = source.get('local_path') or f"{SRC_DIR}/{source['latest_download']}"
	# Load zipfile and duckdb
	with zipfile.ZipFile(source_path, 'r') as zip, duckdb.connect(':memory:') as db:
		# Route DuckDB spill files to canopy temp directory
		db.execute(f"SET temp_directory = '{TMP_DIR}'")
		# Load the initial tsv files
		taxa_tsv = db.read_csv(zip.open('taxon.txt'), parallel=True, header=False, quotechar='', delimiter="\t", names=['taxonID', 
			'modified', 'verbatimTaxonRank', 'scientificName', 'family', 'genus', 'specificEpithet', 'infraspecificEpithet', 
			'scientificNameAuthorship', 'nomenclaturalStatus', 'rightsHolder', 'namePublishedInYear', 'nomenclaturalCode', 
			'taxonRemarks', 'bibliographicCitation', 'language', 'class', 'references', 'license', 'rights', 'namePublishedIn', 
			'taxonRank', 'kingdom', 'phylum', 'parentNameUsageID', 'acceptedNameUsageID', 'originalNameUsageID', 'taxonomicStatus', 
			'source', 'order', 'dynamicProperties'])
		reference_tsv = db.read_csv(zip.open('reference.txt'),parallel=True, header=False, delimiter="\t", names=[
			'coreID','identifier', 'rights', 'date', 'type', 'creator', 'bibliographicCitation', 'license', 'source', 'title', 'rightsHolder', 'taxonRemarks', 'subject'])
		# Create merged table by selecting all fields we're interested and create placeholders for further data
		db.execute(f"""
			CREATE TABLE powo AS SELECT 
				-- IPNI LSID stripped to bare ID, ~1.44M rows. Core plant authority paired with WCVP
				replace(taxonID,'urn:lsid:ipni.org:names:','') AS id_raw,
				scientificName AS name_raw,
				lower(scientificName) AS name_clean,
				-- verbatim rank includes -4 non-IPNI data not in taxonRank
				verbatimTaxonRank AS rank_raw,
				CAST(NULL AS VARCHAR) AS rank_clean,
				lower(kingdom) AS kingdom,
				lower(phylum) AS phylum,
				lower(class) AS class,
				lower("order") AS order,
				lower(family) AS family,
				lower(genus) AS genus,
			 	REPLACE(parentNameUsageID, 'urn:lsid:ipni.org:names:', '') AS parent_raw,
				trim(scientificNameAuthorship) AS author_raw,
				-- Heterotypic_Synonym ~660k, Accepted ~429k, Homotypic_Synonym ~300k
			 	taxonomicStatus AS status_raw,
				-- extracted but not fused currently
			 	substring(trim(REGEXP_EXTRACT(namePublishedIn, { publication_filter }, 1)),1,120) AS publication_short,
				CAST(substring(namePublishedInYear::VARCHAR, 1, 4) AS USMALLINT) AS year,
				-- WCVP bridge ID from source field
			 	replace(source,'wcvp:','') AS wcvp_id,
				-- boolean trait flags from dynamicProperties JSON lifeform field
				contains(dynamicProperties->>'lifeform','annual') AS annual,
				contains(dynamicProperties->>'lifeform','perennial') AS perennial				
			FROM taxa_tsv 
		""")
		# Debug: inspect raw table before cleanup
		db.sql(f"SELECT * FROM powo").show(max_rows=100)
		# Log
		print(f"IMPORT : Loaded {db.execute('SELECT COUNT(*) FROM ' + source['name']).fetchone()[0]:,} entries from { source['name'] } csv")
		# Find hybrids
		find_hybrids(db,source)  
		# Remove ranks
		strip_rank_from_name(db,source) 
		# Generic cleanup
		name_cleanup(db,source)
		# Build ranks
		build_rank_and_status(db,source)
		# Final validation
		validate(db,source)
		# Write to disc
		write_to_disc(db,source)
