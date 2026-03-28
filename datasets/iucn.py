#
#		IUCN Data Importer
#		Source data updated about yearly
#
#		Highlights: Most important endangered species list and vernacular names in many languages

# TODO: diospyros sp. nov. 'randrianaivoi' iucn/198173093 vs ipni/77075436-1
# TODO: https://www.iucnredlist.org/species/120941671/120980128

# Internal
from .. import SRC_DIR, settings

# File handling
import zipfile
from ..utils.filehandlers import fetch

# DB
import duckdb
from ..utils.queries import strip_author_from_name, name_cleanup, find_hybrids, build_rank_and_status, validate, write_to_disc, language_mappings

source = {
    "name": "iucn",
    "url": "https://hosted-datasets.gbif.org/datasets/iucn/iucn-latest.zip",
	"citation": '<a href="https://www.iucn.org" class="medium">IUCN</a>, International Union for Conservation of Nature. The IUCN Red List of Threatened Species (Version YYYY). <a href="https://www.iucnredlist.org" class="medium">https://www.iucnredlist.org</a>'
}

# Main function called as asyncio Task from run.py
async def update_iucn(session):
	print(f"IMPORT : ############### Starting IUCN Update  ###############")
	update_available = await fetch(session, source)
	# See if we have an update and if yes process it
	if (update_available or settings.FORCE) and not settings.DOWNLOAD_ONLY: process_iucn(source)
	# Return the source dict containing processing outcomes
	return source

# Process a fresh sourcefile
def process_iucn(source: dict):
	print(f"IMPORT : Starting to process { source['latest_download'] }...")  
	# Load zipfile and duckdb
	with zipfile.ZipFile(f"{SRC_DIR}/{source['latest_download']}", 'r') as zip, duckdb.connect(':memory:') as db:
		# Load the initial tsv file
		taxa_csv = db.read_csv(zip.open('taxon.txt'), parallel=True, header=False, delimiter="\t", names=[
			'id','scientificName', 'kingdom', 'phylum', 'class', 'order', 'family', 'genus', 'specificEpithet', 'scientificNameAuthorship', 
			'taxonRank', 'infraspecificEpithet', 'taxonomicStatus', 'acceptedNameUsageID', 'bibliographicCitation', 'references'])
		# Create merged table by selecting all fields we're interested and create placeholders for further data
		db.execute(f"""
			CREATE TABLE { source['name'] } 
				AS (SELECT 
					-- taxon.txt internal ID, ~80k rows after kingdom filter
					CAST(id AS UINTEGER) AS id_raw, 
					scientificName AS name_raw,		
			 		lower(scientificName) AS name_clean,
					scientificNameAuthorship as author_raw,
					-- species/subspecies dominant, ~64k species + ~1k subspecies
					taxonRank AS rank_raw, 
					CAST(NULL AS VARCHAR) AS rank_clean,  
					taxonomicStatus as status_raw,	
			 		lower(kingdom) AS kingdom,
			 		lower(phylum) AS phylum,
			 		lower(taxa.class) AS class,
			 		lower(taxa.order) AS order,
			 		lower(family) AS family,
			 		lower(genus) AS genus,
					-- assessment ID from references URL for direct Red List links
					CAST(NULLIF(regexp_extract(taxa.references, '/species/\\d+/(\\d+)', 1), '') AS UINTEGER) AS iucn_assessment
					FROM taxa_csv AS taxa
					-- synonyms have _1 suffix in id, filter them out
			 		WHERE NOT contains(id::VARCHAR,'_') AND (taxa.kingdom == 'PLANTAE' OR taxa.kingdom == 'FUNGI')
				);
		""")
		# Log
		print(f"IMPORT : Loaded { db.execute('SELECT COUNT(*) FROM ' + source['name']).fetchone()[0]:,} plants and fungi from { source['name'] } csv")
		# Remove author from name first
		strip_author_from_name(db,source)
		# We shouldn't have many hybrids but still run this to remove '×'s from name_clean
		find_hybrids(db,source)
		# Generic cleanup
		name_cleanup(db,source)
		# Build ranks
		build_rank_and_status(db,source)
		# Fetch all vernacular names below
		iucn_vernacular(zip,db)
		# Fetch the IUCN status'
		iucn_status(zip,db)
		# Final validation
		validate(db,source)
		# Write to disc
		write_to_disc(db,source)

# IUCN vernacular names with aggressive cleanup for messy source data, ~21% of taxa get names
# Note: Nov 2025 schema change swapped isPreferredName and language column positions
def iucn_vernacular(zip: zipfile.ZipFile, db: duckdb.DuckDBPyConnection):
	# vernacularname.txt is headerless — column order verified against meta.xml
	vernacular_csv = db.read_csv(zip.open('vernacularname.txt'), parallel=True, header=False, null_padding=True, delimiter="\t", 
		names=['id','isPreferredName','vernacularName','language'])
	
	try:
		# Clean and filter vernacular names into temp staging table
		db.execute("""
			CREATE TEMP TABLE temp_vernacular AS
			SELECT v.id as id_raw, v.language, 
				LOWER(TRIM(REGEXP_REPLACE(
					CASE 
						-- strip "(erronously: ...)" annotations found in some IUCN names
						WHEN v.vernacularName LIKE '%(erronously:%' 
						THEN REGEXP_REPLACE(v.vernacularName, '\\(erronously:[^)]*\\)', '')
						ELSE REGEXP_REPLACE(
							REGEXP_REPLACE(
								-- strip quotes, colons, semicolons that break lang:name format
								REGEXP_REPLACE(
									REPLACE(REPLACE(REPLACE(v.vernacularName, '\"', ''), ':', ' '), ';', ' '),
									-- normalize whitespace around parentheses
									'[ \\t\\n\\r\\f\\v]*\\([ \\t\\n\\r\\f\\v]*', '('
								),
								-- normalize whitespace before closing paren
								'\\s+\\)', ')'
							),
							-- close unclosed parentheses at end of string
							'\\([^)]*$', '\\0)'
						)
					END
				-- collapse remaining whitespace runs to single space
				, '[ \\t\\n\\r\\f\\v]+', ' ', 'g'))) as vernacularName
			FROM vernacular_csv v
			-- only keep rows with valid 3-letter language codes
			WHERE length(language) = 3 
			-- skip "species code" entries that aren't real names
			AND NOT starts_with(lower(v.vernacularName), 'species code')
			-- only keep names for taxa in our filtered plantae/fungi set
			AND EXISTS (SELECT 1 FROM iucn i WHERE i.id_raw = v.id);
		""")

		# Create the ISO language mapping table
		db.execute(f"""
			CREATE TEMP TABLE lang_iso_mapping AS
			WITH mapping_values AS (SELECT * FROM (VALUES {language_mappings} ) AS t(lang3, lang2))
			SELECT lc.language as orig_lang, COALESCE(mv.lang2, lc.language) as iso_lang
			FROM (SELECT DISTINCT language FROM temp_vernacular) lc
			LEFT JOIN mapping_values mv ON LOWER(lc.language) = mv.lang3;
		""")

		# Add vernacular column to iucn table if it doesn't exist
		db.execute("ALTER TABLE iucn ADD COLUMN IF NOT EXISTS vernacular VARCHAR[]")

		# Update iucn table with formatted vernacular names
		rows = db.execute("""
			UPDATE iucn SET vernacular = (
				SELECT array_agg(lower(lm.iso_lang) || ':' || tv.vernacularName)
				FROM temp_vernacular tv
				JOIN lang_iso_mapping lm ON tv.language = lm.orig_lang
				WHERE tv.id_raw = iucn.id_raw
			);
			SELECT COUNT(*) FROM iucn WHERE vernacular IS NOT NULL;
		""").fetchone()[0]

		# Log
		if rows and rows > 0: print(f"IMPORT : Added vernacular names to {rows:,} IUCN entries")

	finally:
		# Clean up temporary tables
		db.execute("DROP TABLE IF EXISTS temp_vernacular")
		db.execute("DROP TABLE IF EXISTS lang_iso_mapping")

def iucn_status(zip: zipfile.ZipFile, db: duckdb.DuckDBPyConnection):
	# Define the initial tsv file
	# countryCode has all NULL values and locality either Global or NULL, occurrenceStatus either Present, NULL or Absent (148 items)
	distribution_csv = db.read_csv(zip.open('distribution.txt'), parallel=True, header=False, delimiter="\t", 
		names=['id','occurrenceStatus','establishmentMeans','countryCode','source','threatStatus','locality'])
	# Add new columns and update them with standardized IUCN codes
	rows = db.execute(f"""
		ALTER TABLE iucn ADD COLUMN IF NOT EXISTS iucn_status VARCHAR;
		UPDATE iucn
		SET
			iucn_status = CASE LOWER(dist.threatStatus)
				WHEN 'least concern' THEN 'LC'
				WHEN 'data deficient' THEN 'DD'
				WHEN 'vulnerable' THEN 'VU'
				WHEN 'endangered' THEN 'EN'
				WHEN 'near threatened' THEN 'NT'
				WHEN 'critically endangered' THEN 'CR'
				WHEN 'extinct' THEN 'EX'
				WHEN 'extinct in the wild' THEN 'EW'
				-- conservation dependent was folded into near threatened in 2001
				WHEN 'conservation dependent' THEN 'NT'
				ELSE NULL
			END
		FROM distribution_csv dist WHERE iucn.id_raw = dist.id;
		
		SELECT COUNT(*) FROM iucn WHERE iucn_status IS NOT NULL
	""").fetchone()[0]
	# Log 
	if rows and rows > 0: print(f"IMPORT : Added iucn_status to { rows } items")
