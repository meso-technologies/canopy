#
#
#		Core dataset we derive mushroom names from
#		IPNI of mushrooms, about 100,000 species
#		
#		Species Fungorum is a project, based at the Royal Botanic Gardens Kew, to produce an effectively complete global checklist of 
# 		organisms belonging to the kingdom Fungi, and to organisms which were previously included in the fungi but are now classified 
# 		in other branches of the tree of life. It is a collaborative project where a small but active proportion of fungal taxonomist, 
# 		via their publications, add to and update the fungal tree of life. With the development of the CoL+ architecture the project is 
# 		simplified by abandoning the thirty-six Global Species Databases and recognizing a core of long standing collaborators as follows: 
# 		Gerald L. Benny, Paul F. Cannon, Pedro C. Crous, Tassilo Feuerer, Yu-ming Ju, Bob W. Lichtwardt (deceased), David W. Minter, 
# 		Lisa C. Offord, Jack D. Rogers, Arthur G. Schüßler, Kerstin Voigt, Chris Walker, Nalin N. Wijayawardene, Marvin C. Williams. 
# 
# 		Species Fungorum is built on top of the global fungal nomenclator Index Fungorum, which is now a contributor of taxonomically 
# 		unevaluated names to CoL+ which are not included in the original Species Fungorum as the AVC name or a synonym.

# Internal
from .. import SRC_DIR, TMP_DIR, settings

# File handling
import zipfile
from ..utils.filehandlers import fetch

# DB
import duckdb
from ..utils.queries import publication_filter, strip_rank_from_name, name_cleanup, find_hybrids, build_rank_and_status, validate, write_to_disc


source = {
    "name": "fungorum",
    "url": "https://api.checklistbank.org/dataset/2073/archive.zip",
	"citation": '<a href="https://www.speciesfungorum.org" class="medium">Species Fungorum</a>, Kirk, P. M. (YYYY). Species Fungorum Plus (MMM YYYY). Royal Botanic Gardens, Kew, Richmond, United Kingdom. <a href="https://doi.org/10.15468/ts7wsb" class="medium">https://doi.org/10.15468/ts7wsb</a>'
}

# Main function called as asyncio Task from run.py
async def update_fungorum(session):
	print(f"IMPORT : ############### Starting Species Fungorum Update  ###############")
	update_available = await fetch(session, source)
	# See if we have an update and if yes process it
	if (update_available or settings.FORCE) and not settings.DOWNLOAD_ONLY: process_fungorum(source)
	# Return the source dict containing processing outcomes
	return source

# Process a fresh source file
def process_fungorum(source: dict):
	print(f"IMPORT : Starting to process { source['latest_download'] }...")  
	# Load zipfile and duckdb
	with zipfile.ZipFile(f"{SRC_DIR}/{source['latest_download']}", 'r') as zip, duckdb.connect(':memory:') as db:
		# Route DuckDB spill files to canopy temp directory
		db.execute(f"SET temp_directory = '{TMP_DIR}'")
		# Load the initial tsv files
		name_tsv = db.read_csv(zip.open('Name.tsv'),parallel=True)
		# "col:ID" is always equal to "col:nameID"
		zipfile.ZipFile(f"{SRC_DIR}/{source['latest_download']}").extractall(TMP_DIR) 
		# We have up to 7 relations per entry
		synonym_tsv = db.read_csv(zip.open('Synonym.tsv'),parallel=True)
		# We have up to 7 relations per entry
		reference_tsv = db.read_csv(zip.open('Reference.tsv'),parallel=True)
		# Create merged table by selecting all fields we're interested and create placeholders for further data
		db.execute(f"""
			CREATE TABLE fungorum AS 
			SELECT 
				-- Name.tsv ID, ~477k rows total
				CAST(n.ID AS UINTEGER) as id_raw,
				-- duplicate of id_raw for cross-linking with CoL/MycoBank
			 	CAST(n.ID AS UINTEGER) as fungorum_id,
				-- ~464k distinct names, 100% populated
				n.scientificName as name_raw,
				lower(n.scientificName) AS name_clean,
				-- ~88k distinct authors, 100% populated
				n.authorship as author_raw,
				-- 84 raw rank values -> 11 clean. species ~374k, variety ~64k, form ~32k
				n.rank as rank_raw,
				CAST(NULL AS VARCHAR) AS rank_clean,
				-- Taxon.tsv hierarchy, only ~33% populated (156k/477k have kingdom)
				lower(t.kingdom) AS kingdom,
				-- 15 phyla, ~156k populated
				lower(t.phylum) AS phylum,
				-- 66 classes, ~151k populated
				lower(t.class) AS class,
				-- 280 orders, ~146k populated
				lower(t.order) AS order,
				-- 1097 families, ~142k populated
				lower(t.family) AS family,
				-- ~13k distinct genera
				lower(t.genus) AS genus,
				-- ~66k distinct species epithets
				lower(t.species) AS species,
				-- Taxon.tsv parentID, currently 0% populated in processed output
			 	t.parentID as parent_raw,
				-- Reference.tsv source -> publication snippet, ~24k distinct, 100% populated
			 	substring(trim(REGEXP_EXTRACT(REGEXP_REPLACE(r.source, '^in ', ''), {publication_filter}, 1)),1,120) AS publication_short,
				-- Reference.tsv year, 100% populated, range 1753-2025
				CAST(r.year AS USMALLINT) AS year,
			FROM name_tsv n
			-- Taxon.tsv has hierarchy but only ~33% of Name.tsv rows have a match
			LEFT JOIN read_csv('{TMP_DIR}/Taxon.tsv',nullstr='NULL') t ON n.ID = t.ID
			-- Reference.tsv provides year and publication source for every name
			LEFT JOIN reference_tsv r ON n.ID = r.ID
		""")
		# Add synonym column from Synonym.tsv, ~57k rows have at least one synonym
		db.execute("""
			ALTER TABLE fungorum 
			-- array of (nameID, status) structs per taxon
			ADD COLUMN synonyms STRUCT(id VARCHAR, type VARCHAR)[];
		""")
		# Populate synonyms by aggregating all Synonym.tsv rows matching each taxon
		db.execute("""
			UPDATE fungorum 
			SET synonyms = (
				-- list() aggregates all synonym relations for this taxon
				SELECT list(struct_pack(
					id := CAST(nameID AS VARCHAR),
					type := CAST("status" AS VARCHAR)
				))
				FROM synonym_tsv 
				WHERE taxonID = fungorum.id_raw
			);
		""")
		# Log
		print(f"IMPORT : Loaded { db.execute('SELECT COUNT(*) FROM ' + source['name']).fetchone()[0] } fungi names from { source['name'] }")
		# TODO: Add a check to see if rank in name matches actual rank
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
