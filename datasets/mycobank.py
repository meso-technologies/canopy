#
#			Mycobank
#			Especially for higher fungal ranks that are not in Index/Species Fungorum
#	
#			MycoBank is an on-line database aimed as a service to the mycological and scientific community by documenting 
# 			mycological nomenclatural novelties (new names and combinations) and associated data. 
# 			Westerijk Fungal Biodiversity Institute.
#

source = {
    "name": "mycobank",
    "url": "https://www.mycobank.org/Images/MBList.zip",
	"citation": '<a href="https://www.uu.nl/en/research/life-sciences/facilities/facilities-for-organism-research/westerdijk-fungal-biodiversity-institute" class="medium">Mycobank</a> Crous, P.W., Gams, W., Stalpers, J.A., Robert, V., Stegehuis, G., & Bensch, K. MycoBank Database Version YYYY-MM-DD. Westerdijk Fungal Biodiversity Institute, Utrecht, Netherlands. <a href="https://www.mycobank.org" class="medium">https://www.mycobank.org</a>'
}

# Internal
from ..utils.log import mesologger
from .. import SRC_DIR, TMP_DIR, settings

# File handling
import zipfile
from ..utils.filehandlers import fetch

# DB
import duckdb
from ..utils.queries import name_cleanup, find_hybrids, build_rank_and_status, validate, write_to_disc

# Main function called as asyncio Task from run.py
async def update_mycobank(session):
	mesologger.info(f"############### Updating Mycobank ###############")
	update_available = await fetch(session, source)
	# See if we have an update and if yes process it
	if (update_available or settings.FORCE) and not settings.DOWNLOAD_ONLY: process_mycobank(source)
	# Return the source dict containing processing outcomes
	return source

def process_mycobank(source: dict):
	# Load zipfile and duckdb
	with duckdb.connect(':memory:') as db:
		# Route DuckDB spill files to canopy temp directory
		db.execute(f"SET temp_directory = '{TMP_DIR}'")
		# Extract the single ... *checks notes* ... Excel file
		# Resolve local source path (already ensured by fetch in S3 mode)
		source_path = source.get('local_path') or f"{SRC_DIR}/{source['latest_download']}"
		zipfile.ZipFile(source_path).extractall(TMP_DIR) 
		mesologger.info(f"Unzipped Mycobank xlsx to {TMP_DIR}")
		# Next step, duckdb doesn't support read_xlsx() in Python yet
		db.execute(f"""CREATE TABLE mb_raw AS SELECT * FROM read_xlsx('{TMP_DIR}/MBList.xlsx');""")
		mesologger.info(f"{TMP_DIR}/MBList.xlsx ingested")
		# Copy to actual table
		db.execute(f"""
			CREATE TABLE mycobank AS SELECT 
				-- MycoBank number, ~545k rows. Supplements Index Fungorum for fungi
				"MycoBank #" AS id_raw,
			 	lower("Taxon name") AS name_clean,
				-- abbreviated authors align better with IPNI/WCVP/POWO for consensus. 95% populated
			 	"Authors (abbreviated)" AS author_raw,
				-- 35 raw rank values -> 23 clean. species ~403k, variety ~55k, form ~24k
			 	"Rank" AS rank_raw,
				-- extracted from free-text year field, 92% populated
			 	CAST(NULLIF(regexp_extract("Year of effective publication", '\\d{{4}}', 0),'') AS USMALLINT) AS year,
				-- Legitimate ~482k, Orthographic variant ~24k, Invalid ~16k, Illegitimate ~15k
			 	"Name status" AS status_raw,
				-- same as id_raw, used for cross-linking with Fungorum/CoL
			 	"MycoBank #" AS fungorum_id
			FROM mb_raw
		""")
		mesologger.info(f"Loaded {db.execute('SELECT COUNT(*) FROM ' + source['name']).fetchone()[0]:,} fungi from { source['name'] }")	
		# db.sql("SUMMARIZE mb_raw").show(max_rows=100)
		# db.sql("SELECT Classification FROM mb_raw").show(max_rows=100)
		# db.sql(f"""SELECT DISTINCT "Classification", COUNT("Classification") FROM mb_raw GROUP BY "Classification" ORDER BY COUNT("Classification") DESC""").show(max_rows=30)
		# db.sql(f"""SELECT DISTINCT "Rank", COUNT("Rank") FROM mb_raw GROUP BY "Rank" ORDER BY COUNT("Rank") DESC""").show(max_rows=30)
		# TODO: Check what those hybrids are
		find_hybrids(db,source)
		# Generic cleanup
		name_cleanup(db,source)
		# Build ranks
		build_rank_and_status(db,source)
		# Final validation
		validate(db,source)
		# Write to disc
		write_to_disc(db,source)
		# db.sql(f"SELECT * FROM mycobank").show(max_rows=200)
