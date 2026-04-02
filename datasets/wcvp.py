#
# 		World Checklist of Vascular Plants
#		Taxon acceptance input, as well as detailed historical tree
#
#		The World Checklist of Vascular Plants (WCVP) is a global consensus view of all known vascular plant 
# 		species (flowering plants, conifers, ferns, clubmosses and firmosses). WCVP aims to represent a global 
# 		consensus view of current plant taxonomy by reflecting the latest published taxonomies while incorporating 
# 		the opinions of taxonomists based around the world. WCVP is built on the nomenclatural data provided by the 
# 		International Plant Names Index (IPNI), which is the product of a collaboration between The Royal Botanic Gardens, 
# 		Kew, The Harvard University Herbaria, and the Australian National Herbarium, combined with the taxonomic data 
# 		provided by an international collaborative programme with a large number of contributors from around the world. 
# 		Our thanks go to the compilers, editors and reviewers of IPNI and WCVP and in particular the thousands of users 
# 		who have contributed corrections over the past decades, improving those data for the global user community.
#
#		All names strictly follow the rules of the International Code of Nomenclature for algae, fungi, and plants (ICN). 
#		The WCVP database is updated daily and refreshed on Plants of the World Online (POWO) every Monday after the next 
# 		Wednesday that an edit was made.
#
#		TODO: Find more frequently updated source (current is ~twice/year, checklistbank has ~weekly)
#		TODO: Check how many Artificial Hybrid and Local Biotype taxon_status are accepted by WFO or POWO
#		TODO: Extract full lifeform_description and climate_description beyond boolean flags
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
    "name": "wcvp",
    "url": "https://sftp.kew.org/pub/data-repositories/WCVP/wcvp.zip",
	"citation": '<a href="https://powo.science.kew.org/about-wcvp" class="medium">WCVP</a> Govaerts, R. (YYYY). The World Checklist of Vascular Plants (WCVP). Version YYYY-MM-DD. Royal Botanic Gardens, Kew, Richmond, United Kingdom. <a href="https://doi.org/10.15468/6h8ucr" class="medium">https://doi.org/10.15468/6h8ucr</a>'
}

# Main function called as asyncio Task from run.py
async def update_wcvp(session):
	print(f"IMPORT : ############### Starting World Checklist of Vascular Plants Update  ###############")
	update_available = await fetch(session, source)
	# See if we have an update and if yes process it
	if (update_available or settings.FORCE) and not settings.DOWNLOAD_ONLY: process_wcvp(source)
	# Return the source dict containing processing outcomes
	return source
    
# Process a fresh source file
def process_wcvp(source: dict):
	print(f"IMPORT : Starting to process { source['latest_download'] }...") 
	# Load zipfile and duckdb
	with zipfile.ZipFile(f"{SRC_DIR}/{source['latest_download']}", 'r') as zip, duckdb.connect(':memory:') as db:
		# Route DuckDB spill files to canopy temp directory
		db.execute(f"SET temp_directory = '{TMP_DIR}'")
		# Load the initial tsv files
		name_csv = db.read_csv(zip.open('wcvp_names.csv'),parallel=True,delimiter="|")
		distribution_csv = db.read_csv(zip.open('wcvp_distribution.csv'),parallel=True,delimiter="|")
		# Create merged table by selecting all fields we're interested and create placeholders for further data
		db.execute(f"""
			CREATE TABLE wcvp AS SELECT 
				-- wcvp_names.csv plant_name_id, ~1.44M rows. Primary plant authority
				plant_name_id as id_raw,
				taxon_name as name_raw,
				lower(taxon_name) AS name_clean,
				-- 36 raw ranks: Species ~1M, Variety ~230k, Subspecies ~73k dominant
				taxon_rank as rank_raw,
				CAST(NULL AS VARCHAR) AS rank_clean,
				-- 469 families
			 	lower(family) AS family,
			 	lower(genus) AS genus,
			 	lower(species) AS species,
				-- hybrid detection from genus_hybrid / species_hybrid marker columns
			 	((genus_hybrid = '×') OR (species_hybrid = '×')) AS hybrid,
				CASE 
					WHEN genus_hybrid = '×' THEN 'genus'
					WHEN species_hybrid = '×' THEN 'species'
					ELSE NULL
				END AS hybrid_type,
				hybrid_formula,
			 	parent_plant_name_id AS parent_raw,
				-- primary_author preferred over taxon_authors for cleaner consensus
				trim(primary_author) as author_raw,
				-- Accepted ~429k, Synonym ~871k, problematic ~87k, Unplaced ~40k
			 	taxon_status AS status_raw,
				-- place_of_publication, extracted but not fused currently
			 	substring(place_of_publication,1,120) AS publication_short,
				CAST(NULLIF(regexp_extract(first_published, '\\d{{4}}', 0),'') AS USMALLINT) AS year,
				-- ~30% reviewed
			 	(reviewed = 'Y') AS reviewed,
				-- cross-links to IPNI/POWO, ~90% populated
			 	ipni_id,
			 	powo_id,
				-- boolean trait flags from lifeform_description text, ~27% coverage
				contains(lifeform_description,'annual') AS annual,
				contains(lifeform_description,'perennial') AS perennial	
			FROM name_csv 
		""")
		# Log
		print(f"IMPORT : Loaded {db.execute('SELECT COUNT(*) FROM ' + source['name']).fetchone()[0]:,} entries from { source['name'] } csv")
		# Add locations
		db.execute(""" 
			ALTER TABLE wcvp ADD COLUMN IF NOT EXISTS native_to VARCHAR[];
			WITH habitats AS (
				SELECT plant_name_id, list_distinct(array_agg(lower(COALESCE(area_code_l3,region_code_l2::VARCHAR,continent_code_l1::VARCHAR)))) AS locations
			 	FROM distribution_csv WHERE introduced = 0 GROUP BY plant_name_id )
			UPDATE wcvp SET native_to = h.locations FROM habitats h WHERE wcvp.id_raw = h.plant_name_id;
		""") 
		print(f"IMPORT : Added native habitats to {db.execute('SELECT COUNT(*) FROM ' + source['name'] + ' WHERE native_to IS NOT NULL;').fetchone()[0]:,} { source['name'] } rows")
		# Add locations
		db.execute(""" 
			ALTER TABLE wcvp ADD COLUMN IF NOT EXISTS regions VARCHAR[];
			WITH habitats AS (
				SELECT plant_name_id, list_distinct(array_agg(lower(COALESCE(area_code_l3,region_code_l2::VARCHAR,continent_code_l1::VARCHAR)))) AS locations
			 	FROM distribution_csv GROUP BY plant_name_id )
			UPDATE wcvp SET regions = h.locations FROM habitats h WHERE wcvp.id_raw = h.plant_name_id;
		""") 
		print(f"IMPORT : Added regions to {db.execute('SELECT COUNT(*) FROM ' + source['name'] + ' WHERE regions IS NOT NULL;').fetchone()[0]:,} { source['name'] } rows")
		# Remove ranks
		strip_rank_from_name(db,source)  
		# Find hybrids
		find_hybrids(db,source)  
		# Generic cleanup
		name_cleanup(db,source)
		# Build ranks
		build_rank_and_status(db,source)
		# Final validation
		validate(db,source)
		# Write to disc
		write_to_disc(db,source)
