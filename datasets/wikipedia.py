#
# 		Wikipedia GraphQL clients
#
from ..utils.log import mesologger
import os, requests, time
from .. import settings, API_DATA_DIR, RELEASES_DIR, TMP_DIR
from ..utils.filehandlers import get_latest_release, get_file
# Load shared storage proxy for local/S3 transparent parquet operations
from ..utils.s3 import storage
# Data processing
import duckdb, pyarrow

def update_wikipedia(release=None):
	mesologger.info(f"Updating local copy of Wikipedia data")
	# If we jumped directly here and have no release, grab the latest to look up wikidata Q identifiers
	if not release: 
		release = get_latest_release(RELEASES_DIR)
		if not release:
			mesologger.warning(f"No release candidate found, aborting")
			return	
		else: mesologger.warning(f"No release provided, falling back on latest staging release {release['version']}")	
	# Spawn db
	with duckdb.connect(':memory:') as db:
		# Route DuckDB spill files to canopy temp directory
		db.execute(f"SET temp_directory = '{TMP_DIR}'")
		# Configure DuckDB S3 settings when reading/writing parquet in object storage
		if storage.is_s3(): storage.configure_duckdb(db)
		# Check if we have any wikipedia abstracts at all
		if not storage.exists(os.path.join(API_DATA_DIR, f"wikipedia_abstracts.parquet")): 
			mesologger.info(f"No Wikipedia abstracts found, building dataset from scratch")
			build_abstracts(release,db)
		else:
			mesologger.info(f"Updating existing wikidata abstracts")
			build_abstracts(release,db,True)				

# Build a new abstract dataset from scratch
def build_abstracts(release: dict, db: duckdb.DuckDBPyConnection, update_only: bool=False):
	# Grab the basic data, one ID only if we still have duplicates from our importer
	meso_parquet = db.read_parquet(storage.parquet_url(os.path.join(RELEASES_DIR, f"{release['version']}/{release['version']}.parquet")))
	# Register download function as pyarrow UDF
	db.create_function('download_abstracts',download_abstracts,['VARCHAR'],'VARCHAR',type='arrow',side_effects=True)
	# Spawn a new table if we don't have one
	if not update_only or not storage.exists(os.path.join(API_DATA_DIR, f"wikipedia_abstracts.parquet")):
		db.execute(f"""CREATE TABLE wikipedia_abstracts AS SELECT id_meso, name_consensus, wikidata_id, wikipedia_page AS en_page_title FROM meso_parquet WHERE accepted;""")
		mesologger.info(f"Loaded {db.execute("SELECT COUNT(*) FROM wikipedia_abstracts").fetchone()[0]:,} candidates from release {release['version']}.parquet into new table")
		# Download up to 20 abstracts at a time, via page title first
		db.execute(f"""
			ALTER TABLE wikipedia_abstracts ADD COLUMN IF NOT EXISTS abstract VARCHAR;
			ALTER TABLE wikipedia_abstracts ADD COLUMN IF NOT EXISTS last_checked TIMESTAMPTZ;
			UPDATE wikipedia_abstracts SET 
				abstract = NULLIF(download_abstracts(trim(en_page_title)),''),
				last_checked = transaction_timestamp()
			WHERE en_page_title IS NOT NULL
			{ 'LIMIT 100' if settings.DEBUG else '' };
		""")
		by_id_count = db.execute("SELECT COUNT(*) FROM wikipedia_abstracts WHERE abstract IS NOT NULL").fetchone()[0]
		mesologger.info(f"Downloaded {by_id_count:,} abstracts by en_page_title")
		mesologger.info(f"Fresh download via Wikipedia API complete, {db.execute("SELECT COUNT(*) FROM wikipedia_abstracts WHERE abstract IS NOT NULL").fetchone()[0]:,} abstracts found")
	# Otherwise update our existing table
	else:
		wikipedia_parquet = db.read_parquet(storage.parquet_url(os.path.join(API_DATA_DIR, f"wikipedia_abstracts.parquet")))	
		db.execute(f"""CREATE TABLE wikipedia_abstracts AS SELECT * FROM wikipedia_parquet;""")
		existing_rowcount = db.execute("SELECT COUNT(*) FROM wikipedia_abstracts").fetchone()[0]
		mesologger.info(f"Loaded {existing_rowcount:,} rows from existing wikipedia_abstracts.parquet")
		# Load meso data 
		db.execute(f"""
			CREATE TABLE meso AS SELECT ANY_VALUE(id_meso) AS id_meso, ANY_VALUE(name_consensus) AS name_consensus, wikidata_id, ANY_VALUE(wikipedia_page) AS en_page_title
			FROM meso_parquet WHERE accepted GROUP BY wikidata_id;""")
		# See if we have any new values
		db.execute(f"""
			-- Added to schema later
			ALTER TABLE wikipedia_abstracts ADD COLUMN IF NOT EXISTS name_consensus VARCHAR;
			-- Load any missing names
			UPDATE wikipedia_abstracts SET name_consensus = m.name_consensus FROM meso m WHERE wikipedia_abstracts.name_consensus IS NULL AND wikipedia_abstracts.wikidata_id = m.wikidata_id;
			-- Load any new values by ID
			INSERT INTO wikipedia_abstracts BY NAME SELECT id_meso, name_consensus, wikidata_id, en_page_title
			FROM meso m WHERE m.wikidata_id IS NOT NULL AND m.wikidata_id NOT IN (SELECT wikidata_id FROM wikipedia_abstracts WHERE wikidata_id IS NOT NULL);
		""")
		# Count & log
		updated_rowcount = db.execute("SELECT COUNT(*) FROM wikipedia_abstracts").fetchone()[0]
		mesologger.info(f"Added {int(updated_rowcount-existing_rowcount):,} additional candidates by ID")
		current_count = db.execute("SELECT COUNT(*) FROM wikipedia_abstracts WHERE abstract IS NOT NULL;").fetchone()[0]
		# Look for ID matches 
		db.execute(f"""
			UPDATE wikipedia_abstracts SET 
				abstract = NULLIF(download_abstracts(en_page_title),''),
				last_checked = transaction_timestamp()
			WHERE en_page_title IS NOT NULL AND (last_checked IS NULL OR last_checked < transaction_timestamp() - INTERVAL '2 weeks')
			{ 'LIMIT 100' if settings.DEBUG else '' };
		""")
		by_id_count = db.execute("SELECT COUNT(*) FROM wikipedia_abstracts WHERE abstract IS NOT NULL").fetchone()[0]
		differential = int(by_id_count-current_count)
		if differential > 0: mesologger.info(f"Downloaded {differential:,} new or updated abstracts by en_page_title")
		mesologger.info(f"Update via Wikipedia API complete, we currently have {db.execute("SELECT COUNT(*) FROM wikipedia_abstracts WHERE abstract IS NOT NULL").fetchone()[0]:,} abstracts")
	# Extended logging
	if settings.VERBOSE: db.sql("SELECT * FROM wikipedia_abstracts").show(max_rows=100)
	# Write parquet
	filename = os.path.join(API_DATA_DIR, f"wikipedia_abstracts")
	output_path = storage.parquet_url(filename + '.parquet')
	db.execute(f"COPY wikipedia_abstracts TO '{output_path}' (FORMAT PARQUET)")
	mesologger.info(f"Saved Wikipedia abstracts file to {output_path}")
	# Write .csv as well if we have our flag set
	if settings.CSV: db.sql(f"SELECT * FROM wikipedia_abstracts").write_csv(filename + '.tsv',sep='\t') 

# UDF to look up chunks of meso UUIDS, 2048 at a time
abstract_count = 0
def download_abstracts(scalars: pyarrow.StringArray):
	# Counter
	global abstract_count
	# Wikipedia API allows 20 page titles per request
	batch_size = 20
	# Convert scalars to python list
	pageset = scalars.to_pylist()
	# Use dict to track title->abstract mapping
	results = {}  
	# Fetch session
	session = requests.Session()
	# Auth
	headers = {
		'User-Agent': 'Canopy Taxonomy Pipeline/1.0 (opensource@meso.cloud)'
	}
	# Add optional token when configured
	if settings.WIKIDATA_TOKEN: headers['Authorization'] = settings.WIKIDATA_TOKEN
	session.headers.update(headers)
	# Iterate
	for i in range(0, len(pageset), batch_size):
        # Rate limit
		if i > 0: time.sleep(0.2)
		# Get batch of id's 
		batch = pageset[i:i+batch_size]
		# Use sections instead of full extract - drops the exlimit warning
		# Also make sure to follow redirects
		url = f"https://en.wikipedia.org/w/api.php?action=query&prop=extracts&exintro=true&explaintext=true&redirects&titles={'|'.join(batch)}&format=json"
        # Make request with backoff logic
		max_retries = 100
		retry_count = 0
		backoff_time = 1 
		while retry_count < max_retries:
			response = session.get(url)
			if response.status_code == 200: break
			# If we get rate limited, wait and retry
			mesologger.info(f"Rate limited, backing off for {backoff_time}s")
			mesologger.warning(response.status_code)
			mesologger.info(f"Response {response}")
			time.sleep(backoff_time)
			backoff_time *= 2  # Exponential backoff
			retry_count += 1
		# Extract data
		data = response.json()
		if 'continue' in data: mesologger.warning(f"Got continuation warning {data['continue']}")
		# If we have any results
		if 'query' in data and 'pages' in data['query']:
			# Pointers
			q = data['query']
			pages = q['pages']
			# Should not happen at all
			if 'normalized' in q: mesologger.warning(f"WARNING, NORMALIZATION NOT IMPLEMENTED YET { q['normalized'] }")
			# Add mapping table for redirects
			redirects = { r['to']: r['from'] for r in q.get('redirects', []) }
			# Iterate through results
			for page_id, page_info in pages.items():
				title = page_info.get('title')
				# Only store if we got content
				if title and 'extract' in page_info: results[title] = page_info.get('extract', '')
				else: mesologger.warning(f"WARNING, TITLE MISSING {page_info}")	
				# Also add any originals of redirects
				if title in redirects: results[redirects[title]] = page_info.get('extract', '')
		else: mesologger.warning(f"WARNING, QUERY DATA MISSING {data}")			
	# Iterate for next round
	abstract_count += len(pageset)
	# Log
	mesologger.info(f"Downloaded {abstract_count} abstracts", extra={'sameline': True})
	# Turn lookup object results back into response list
	return [results.get(title, '') for title in pageset]
	