#		The occurence data is fantastic, but the backbone is terrible. We basically only use it
#		for seeing if GBIF also accepted a taxon (if we have at least one other source),
#		to improve our ocurence consolidation with basic lookups, and for vernauclar names.
# 
# 		An international network and data infrastructure funded by the world's governments and aimed at providing anyone, 
# 		anywhere, open access to data about all types of life on Earth.
#
#		Great notes at: https://data-blog.gbif.org/post/gbif-backbone-taxonomy/
#		The backbone is built from other checklists. These include:
#   	55 authority checklists, a checklist generated from the type specimens shared on GBIF, 
# 		[EDIT 2024-04-30 the latest versions of the Backbone taxonomy no longer include this checklist as it contained a 
# 		number of issues and was difficult to update as it relied on many different occurrence publishers.]
#    	two large sources for stable Operational Taxonomic Units (OTUs): iBOL Barcode Index Numbers and the 
# 		UNITE Species Hypothesis identifiers, and any checklist shared by PLAZI.org on GBIF (currently 27,054 but not all 
# 		these were available when the backbone was generated).

source = {
    "name": "gbif",
    "url": "https://hosted-datasets.gbif.org/datasets/backbone/current/backbone.zip",
	"citation": '<a href="https://www.gbif.org" class="medium">GBIF</a>, Döring, M. (YYYY). GBIF Backbone Taxonomy. Version YYYY-MM-DD. GBIF Secretariat, Copenhagen, DK. <a href="https://doi.org/10.15468/39omei" class="medium">https://doi.org/10.15468/39omei</a>'
}

# Internal
from ..utils.log import mesologger
from .. import SRC_DIR, TMP_DIR, settings

# File handling
import zipfile
from ..utils.filehandlers import fetch

# DB
import duckdb
from ..utils.queries import strip_author_from_name, name_cleanup, find_hybrids, build_rank_and_status, validate, write_to_disc, language_mappings

# Main function called as asyncio Task from run.py
async def update_gbif(session):
	mesologger.info(f"############### Starting GBIF Update ###############")
	update_available = await fetch(session, source)
	# See if we have an update and if yes process it
	if (update_available or settings.FORCE) and not settings.DOWNLOAD_ONLY: process_gbif(source)
	# Return the source dict containing processing outcomes
	return source

def process_gbif(source: dict):
	# Resolve local source path (already ensured by fetch in S3 mode)
	source_path = source.get('local_path') or f"{SRC_DIR}/{source['latest_download']}"
	# Load zipfile and duckdb
	with zipfile.ZipFile(source_path, 'r') as zip, duckdb.connect(':memory:') as db:
		# Route DuckDB spill files to canopy temp directory
		db.execute(f"SET temp_directory = '{TMP_DIR}'")
		# Load the initial tsv files
		taxa_tsv = db.read_csv(zip.open('Taxon.tsv'),parallel=True,ignore_errors=True)
		distribution_tsv = db.read_csv(zip.open('Distribution.tsv'),parallel=True,ignore_errors=True)
		# Source contains weirdly formatted IPNI etc links
		description_tsv = db.read_csv(zip.open('Description.tsv'),parallel=True)
		mesologger.info(f"Unpacked gbif archive")
		# Build initial table
		db.execute(f"""
			CREATE TABLE gbif AS SELECT 
				-- GBIF backbone taxon ID, ~2.98M rows after kingdom filter
				CAST(taxonID AS UINTEGER) AS id_raw,
			 	lower(scientificName) AS name_clean,
			 	CAST(parentNameUsageID AS UINTEGER) AS parent_raw,
				-- Keep acceptedNameUsageID so synonym occurrences can map to accepted GBIF keys
				CAST(acceptedNameUsageID AS UINTEGER) AS accepted_raw,
			 	scientificNameAuthorship AS author_raw,
			 	taxonRank AS rank_raw,
				-- hierarchy from Taxon.tsv, noisy but broad coverage
				lower(kingdom) AS kingdom,
				lower(phylum) AS phylum,
				lower(class) AS class,
				lower("order") AS "order",
				lower(family) AS family,
				lower(genus) AS genus,
				-- taxonomicStatus: accepted/synonym/doubtful etc
			 	taxonomicStatus AS status_raw,
			FROM taxa_tsv t
			WHERE kingdom IN ('Plantae','Fungi')
		""")
		# Log
		mesologger.info(f"Loaded {db.execute('SELECT COUNT(*) FROM ' + source['name']).fetchone()[0]:,} plants & fungi from { source['name'] }")
		# Distribution.tsv: TDWG codes or 2-letter country codes, same pattern as CoL
		# native_to ~117k taxa (native only), regions ~601k taxa (all establishment means)
		db.execute("""
			ALTER TABLE gbif ADD COLUMN IF NOT EXISTS native_to VARCHAR[];
			WITH habitats AS (SELECT taxonID, list_distinct(array_agg(
				CASE 
					-- strip tdwg: prefix and -oo country suffix
					WHEN lower(locationID) LIKE 'tdwg:%' THEN replace(replace(lower(locationID), 'tdwg:', ''),'-oo','')
					-- bare 2-letter ISO country codes
					WHEN length(countryCode) = 2 THEN lower(countryCode)
					ELSE NULL
				END)) AS locations
				FROM distribution_tsv WHERE establishmentMeans = 'native' GROUP BY taxonID 
			)
			UPDATE gbif SET native_to = h.locations FROM habitats h WHERE gbif.id_raw = h.taxonID;
		""")
		mesologger.info(f"Added native habitats to {db.execute('SELECT COUNT(*) FROM ' + source['name'] + ' WHERE native_to IS NOT NULL;').fetchone()[0]:,} { source['name'] } rows")
		# Same aggregation but without native filter
		db.execute("""
			ALTER TABLE gbif ADD COLUMN IF NOT EXISTS regions VARCHAR[];
			WITH habitats AS (SELECT taxonID, list_distinct(array_agg(
				CASE 
					WHEN lower(locationID) LIKE 'tdwg:%' THEN replace(replace(lower(locationID), 'tdwg:', ''),'-oo','')
					WHEN length(countryCode) = 2 THEN lower(countryCode)
					ELSE NULL
				END)) AS locations
				FROM distribution_tsv GROUP BY taxonID 
			)
			UPDATE gbif SET regions = h.locations FROM habitats h WHERE gbif.id_raw = h.taxonID;
		""")
		mesologger.info(f"Added regions to {db.execute('SELECT COUNT(*) FROM ' + source['name'] + ' WHERE regions IS NOT NULL;').fetchone()[0]:,} { source['name'] } rows")
		# GBIF embeds author in scientificName, strip it out
		strip_author_from_name(db, source)
		# GBIF appends year to author like "L., 1753" — split into year column (~4% populated)
		db.execute(f"""
			ALTER TABLE gbif ADD COLUMN IF NOT EXISTS year USMALLINT;
			UPDATE gbif SET 
				author_raw = regexp_replace(author_raw, ', [0-9]{{4}}$', ''),
				year = regexp_extract(author_raw, ', ([0-9]{{4}})$', 1)
			WHERE regexp_matches(author_raw, '.*?, [0-9]{{4}}$')
		""")
		# Fetch all vernacular names below
		gbif_vernacular(zip,db)
		# Clean up the ranks
		build_rank_and_status(db, source)
		# Find hybrids
		find_hybrids(db, source)
		# Clean up names
		name_cleanup(db, source)
        # Check integrity
		validate(db, source)
		# Write to disc
		write_to_disc(db,source)

# Add vernacular names from VernacularName.tsv, ~76k taxa get names (~2%)
# Names split on semicolons/colons, language codes normalized to 2-letter ISO
def gbif_vernacular(zip: zipfile.ZipFile, db: duckdb.DuckDBPyConnection):
	vernacular_tsv = db.read_csv(zip.open('VernacularName.tsv'),parallel=True)
	db.execute(f"""
		ALTER TABLE gbif ADD COLUMN IF NOT EXISTS vernacular VARCHAR[];
		-- 3-letter to 2-letter ISO language mapping
		WITH mapping_values AS (SELECT * FROM (VALUES {language_mappings}) AS t(lang3, lang2)),
		-- split semicolon/colon-separated names into individual rows
		names AS (
			SELECT v.taxonId, COALESCE(mv.lang2, v.language) as iso_lang, unnest(split(regexp_replace(v.vernacularName,'[;:]', ',', 'g'),',')) AS name
			FROM vernacular_tsv v LEFT JOIN mapping_values mv ON v.language = mv.lang3
			-- only keep names for taxa in our filtered plantae/fungi set
			WHERE EXISTS (SELECT 1 FROM gbif g WHERE g.id_raw = v.taxonId))
		-- aggregate into lang:name array per taxon
		UPDATE gbif g SET vernacular = (
			SELECT array_agg(n.iso_lang || ':' || n.name)
			FROM names n WHERE g.id_raw = n.taxonId AND n.iso_lang IS NOT NULL GROUP BY n.taxonId
		);
	""")
	db.sql(f"""SELECT id_raw, name_clean, vernacular FROM gbif WHERE vernacular IS NOT NULL ORDER BY len(vernacular) DESC;""").show(max_rows=100)
