#
#		iNaturalist Data Importer
#		Source data updated about monthly
#
#		Highlights: great vernacular names in many languages
#

# TODO: Check hybrid formulas and VernacularNames-hybrid.csv
# TODO: Get more external IDs out of references column

# Settings 
from .. import SRC_DIR, settings

# File handling
from ..utils.filehandlers import fetch
import zipfile
import re

# Database
import duckdb
from ..utils.queries import name_cleanup, find_hybrids, build_rank_and_status, validate, write_to_disc, language_mappings

# TODO: https://www.inaturalist.org/taxa/403435 Raoulia ×gibbsii https://biotanz.landcareresearch.co.nz/scientific-names/853adff2-f633-4d83-8264-e2a4b29c9705
# TODO: https://www.inaturalist.org/taxa/476184-Myoporum-aff-insulare

source = {
    "name": "inaturalist",
    "url": "https://www.inaturalist.org/taxa/inaturalist-taxonomy.dwca.zip",
	"citation": '<a href="https://www.inaturalist.org" class="medium">iNaturalist</a>, iNaturalist Contributors. iNaturalist Backbone Data. Version YYYY-MM-DD. iNaturalist. <a href="https://www.inaturalist.org" class="medium">https://www.inaturalist.org</a>'
}

# Main function called as asyncio Task from run.py
async def update_inaturalist(session):
	print(f"IMPORT : ############### Starting iNaturalist Update ###############")
	update_available = await fetch(session, source)
	# See if we have an update and if yes process it
	if (update_available or settings.FORCE) and not settings.DOWNLOAD_ONLY: process_inaturalist(source)
	# Return the source dict containing processing outcomes
	return source

# Process a fresh sourcefile
def process_inaturalist(source: dict):
	print(f"IMPORT : Starting to process { source['latest_download'] }...")  
	# Load zipfile and duckdb
	with zipfile.ZipFile(f"{SRC_DIR}/{source['latest_download']}", 'r') as zip, duckdb.connect(':memory:') as db:
		# Load the initial csv file, turning off strict as the 'references' field has a bunch of errors (URLs with commas, quotes etc)
		taxa_csv = db.read_csv(zip.open('taxa.csv'), parallel=False, strict_mode=False)
		# Create merged table by selecting all fields we're interested and create placeholders for further data
		db.execute(f"""
			CREATE TABLE { source['name'] } 
				AS (SELECT 
					CAST(id AS UINTEGER) AS id_raw, 
					CAST(replace(parentNameUsageID,'https://www.inaturalist.org/taxa/','') AS UINTEGER) AS parent_raw,
					scientificName AS name_raw,	
					-- Automatically mark entries in iNat as accepted by iNat
					'accepted' AS status_raw,
			 		lower(scientificName) AS name_clean,
					taxonRank AS rank_raw, 	
					CAST(NULL AS VARCHAR) AS rank_clean,  	
			 		lower(kingdom) AS kingdom,
			 		lower(phylum) AS phylum,
			 		lower(taxa.class) AS class,
			 		lower(taxa.order) AS order,
			 		lower(family) AS family,
			 		lower(genus) AS genus,
			 		-- specificEpithet,
			 		-- infraspecificEpithet,
					-- taxa.references AS links_raw,
					-- External IDs, parsed from references
					-- CAST(NULL AS UINTEGER) AS col_id, 
					-- CAST(NULL AS UINTEGER) AS eol_id, 
					-- CAST(NULL AS VARCHAR) AS ipni_id, 
					FROM taxa_csv AS taxa
			 		WHERE taxa.kingdom == 'Plantae' OR taxa.kingdom == 'Fungi'
				);
		""")
		# db.sql(f"SELECT DISTINCT links_raw, COUNT(links_raw) FROM inaturalist GROUP BY links_raw ORDER BY COUNT(links_raw) DESC").show(max_rows=300)
		# Log
		print(f"IMPORT : Loaded { db.execute('SELECT COUNT(*) FROM ' + source['name']).fetchone()[0] } plants and fungi from inaturalist csv")
		# Find hybrids
		find_hybrids(db,source)
		# Generic cleanup
		name_cleanup(db,source)
		# Build ranks
		build_rank_and_status(db,source)
		# Fetch all vernacular names below
		inaturalist_vernacular(zip,db)
		# Extract external database IDs, disabled for now as it's highly unreliable
		# inaturalist_ids(db, source)
		# Final validation
		validate(db,source)
		# Write to disc
		write_to_disc(db,source)

# Iterate VernacularNames-*.csv files (one per language), ~35% of taxa get names
# Each file is messy CSV that needs lenient parsing settings
def inaturalist_vernacular(zip: zipfile.ZipFile, db: duckdb.DuckDBPyConnection):
	db.execute("ALTER TABLE inaturalist ADD COLUMN IF NOT EXISTS vernacular VARCHAR[]")
	# Each VernacularNames-<lang>.csv has names for one language
	for filename in (f for f in zip.namelist() if f.startswith('VernacularNames-')):
		# First peek at the file to check language (faster than DuckDB load)
		with zip.open(filename) as f:
			# Skip header
			next(f)
			# Read first data line
			language = next(f).decode('utf-8').split(',')[2].strip('"')
			# Skip if we have "und"efined language or broken csv (-west-frisian.csv or -unknown.csv)
			if language == 'und' or not re.match(r'^[a-z]{2,3}(?:[-_][A-Z]{2,3})?$', language): continue

		# Lenient CSV settings required — files have unescaped quotes, missing columns etc
		temp_df = db.read_csv(zip.open(filename), parallel=False, ignore_errors=True, strict_mode=False, null_padding=True)
		try:
			# Strip language to base code (zh-CN -> zh, en-GB -> en), split comma-separated names,
			# replace colons with spaces (breaks lang:name format), strip "and allies" taxonomic noise
			db.execute("""
				CREATE TEMP TABLE temp_vernacular AS
				SELECT 
					id::BIGINT as id_raw, 
					unnest(split(replace(replace(replace(lower(vernacularName), ':', ' '),', and allies',''),' and allies',''),',')) as vernacularName,
					split_part(split_part(language, '-', 1), '_', 1) as language
				FROM temp_df WHERE language != 'und'
			""")

			# Get the normalized language
			lang = db.execute("SELECT DISTINCT language FROM temp_vernacular").fetchone()[0]

			# Append lang:name pairs to existing vernacular array, mapping 3-letter to 2-letter ISO
			updated = db.execute(f"""
				UPDATE inaturalist i SET vernacular = COALESCE(i.vernacular, ARRAY[]) ||
					COALESCE((SELECT array_agg(
						COALESCE((SELECT m.lang2 FROM (VALUES {language_mappings}) AS m(lang3, lang2) WHERE m.lang3 = v.language),
						v.language) || ':' || trim(vernacularName))
					FROM temp_vernacular v
					WHERE v.id_raw = i.id_raw), ARRAY[]::VARCHAR[])
				FROM temp_vernacular v
				WHERE i.id_raw = v.id_raw
				RETURNING 1
			""").fetchdf()

			# Log
			print(f"IMPORT : Updated {len(updated)} inaturalist taxa with names in {lang} ({filename})")
		finally:
			# Clean up temp table and view
			db.execute("DROP TABLE IF EXISTS temp_vernacular")
			db.execute("DROP VIEW IF EXISTS temp_df")

# Extract external IDs from references URLs — currently disabled, data is highly unreliable
def inaturalist_ids(db: duckdb.DuckDBPyConnection, source: dict):
	db.execute(f"""
	UPDATE {source['name']}
	SET
		-- Highly unreliable, 382871-1  repeats 20,000x for example 
 		-- ipni_id = NULLIF(REGEXP_EXTRACT(links_raw, '(?:powo\\.science\\.kew\\.org|plantsoftheworldonline\\.org)/taxon/urn:lsid:ipni\\.org:names:([0-9-]+)', 1), ''),
		col_id = NULLIF(REGEXP_EXTRACT(links_raw, 'catalogueoflife\\.org/annual-checklist/details/species/id/([0-9]{{1,10}})', 1), ''),
		eol_id = NULLIF(REGEXP_EXTRACT(links_raw, 'eol\\.org/pages/([0-9]{{1,10}})', 1), '')
	WHERE
		links_raw LIKE '%powo.science.kew.org%' OR links_raw LIKE '%catalogueoflife.org%' OR links_raw LIKE '%eol.org%'
	""")
	# Cleanup duplicates in messy inat dataset
	db.execute(f"""
	WITH 
		ipni_dupes AS ( SELECT ipni_id FROM {source['name']} WHERE ipni_id IS NOT NULL GROUP BY ipni_id HAVING COUNT(*) > 1),
		eol_dupes AS ( SELECT eol_id FROM {source['name']} WHERE eol_id IS NOT NULL GROUP BY eol_id HAVING COUNT(*) > 1)
	UPDATE {source['name']} SET 
		ipni_id = CASE WHEN ipni_id IN (SELECT ipni_id FROM ipni_dupes) THEN NULL ELSE ipni_id END,
		eol_id = CASE WHEN eol_id IN (SELECT eol_id FROM eol_dupes) THEN NULL ELSE eol_id END
	""")

	# db.sql(f"SELECT DISTINCT ipni_id, COUNT(ipni_id) FROM {source['name']} GROUP BY ipni_id ORDER BY COUNT(ipni_id) DESC").show(max_rows=30)	
