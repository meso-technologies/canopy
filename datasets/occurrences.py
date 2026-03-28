#
# 		Occurrence Geo Data
#
#		The big one: 200GB zipped parquet file but the parquet files are neatly broken down in chunks
#		which we can easily fetch from the zip file and extract.
#
#		TODO: Surface elevation data
#		TODO: Add eventdate as first observation signal alongside BHL first mention
#
# Basics
import os, io, requests, aiohttp, json, time, asyncio
from datetime import datetime, timezone, timedelta
# Internal
from .. import TMP_DIR, GEO_DIR, settings
from ..utils.downloader import aria_download
# File handling & DB
import duckdb
import zipfile
import polars as pl
# Auth
from aiohttp import BasicAuth
# Default user agent for GBIF
user_agent = 'Meso Plant Database/1.0 (bruno@meso.cloud)'
# URL settings
GBIF_API_HOST = 'https://api.gbif.org/v1/'

# Main function 
async def update_occurrences():
	file = os.path.join(GEO_DIR, f"occurrences.parquet")
	# Skip GBIF API update when credentials are missing
	if not settings.GBIF_USER or not settings.GBIF_PASSWORD:
		if os.path.isfile(file):
			print('IMPORT : GBIF credentials missing, skipping occurrence update and reusing local occurrences.parquet')
			return
		print('IMPORT : GBIF credentials missing, skipping occurrence bootstrap')
		print('IMPORT : Register credentials at https://www.gbif.org/user/profile and set them in canopy/secrets.py')
		return
	# Check if we alrady have processed GBIF data locally
	if not os.path.isfile(file):
		print(f"IMPORT : No processed GBIF occurrence dataset found")
		# Check for raw data
		if not os.path.isfile(os.path.join(TMP_DIR, f"occurrences.zip")):	
			print("IMPORT : Downloading raw GBIF data (200GB, this might take a while)")
			# Start download if we don't have any local data
			# GBIF generally only allows one connection at time, but we try anyway
			await aria_download('occurrences.zip', get_latest_url(), 8, TMP_DIR)
			# Create initial manifest
			with open(os.path.join(GEO_DIR,'manifest.json'),'w') as f: json.dump({ 'initial_download': datetime.now(timezone.utc).isoformat() }, f, indent=4)	
		# Then process
		distill_occurrences()		
	else: 
		print(f"IMPORT : Processed GBIF occurrence data found")	
		# Otherwise get more recent occurrences via API
		await get_latest_occurrences(file)

# Look up latest full dataset version when we bootstrap the initial source dataset
def get_latest_url():
	# Fetch session
	session = requests.Session()
	# Say hi
	session.headers.update({'User-Agent': user_agent})
	# Fetch
	response = session.get("https://www.gbif.org/api/occurrence-snapshots?limit=50&offset=0")
	# Get data
	data = response.json()
	# Sanity
	if not response or response.status_code != 200 or not data or 'results' not in data: return None
	# Iterate
	for dataset in data['results']:
		# Ignore other datasets
		if not dataset.get('request') or dataset['request'].get('format') != 'SIMPLE_PARQUET' or dataset['request'].get('predicate'): continue
		# See if it's succeeded or still in progress
		if not dataset.get('status') == 'SUCCEEDED': continue
		# Final size check, using slightly smaller values than May 1st 2025 as reference as this shouldn't shrink
		if int(dataset.get('size')) < 200845216598 or int(dataset.get('totalRecords')) < 3009166525: continue
		# Return
		return dataset.get('downloadLink')
	
# Kick off and wait for an occurrence update
async def get_latest_occurrences(file):
	# Get auth for entire session
	auth = BasicAuth(settings.GBIF_USER, settings.GBIF_PASSWORD)
	# Spawn async http session
	async with aiohttp.ClientSession(auth=auth,headers={"User-Agent": user_agent}) as session:
		# Get current manifest
		manifest = get_manifest()
		# Sanity
		if not manifest: 
			print(f"IMPORT : No GBIF occurrence manifest found!")
			return
		# Check if we have any reason to update, eitehr because we never requested a download yet or it's less than 24h old
		if 'latest_download_request' in manifest and datetime.now(timezone.utc) - datetime.fromisoformat(manifest['latest_download_request']) <= timedelta(days=1):
			print(f"IMPORT : Already requested occurrence update { manifest.get('current_download_key') } within the last 24 hours")
		# Otherwise request a new dataset download
		else: await request_update_from_gbif(session, manifest)
		# Check if we have a pending update that hasn't been processed yet
		if 'last_processed_download_key' not in manifest or manifest.get('current_download_key') != manifest.get('last_processed_download_key'):
			# Try processing a pending update
			url = await get_gbif_download_url(session, manifest)
			# If we got a URL back
			if url: 
				# Set a filename
				filename = f"occurrence_update.{ manifest.get('current_download_key') }.zip"
				# Download if we didn't yet
				if not os.path.isfile(os.path.join(TMP_DIR, filename)): 
					# Wait up to 10 minutes for file to be ready
					for attempt in range(60):  
						async with session.head(url) as head_resp:
							if head_resp.status == 200:
								content_length = head_resp.headers.get('Content-Length')
								if content_length and int(content_length) > 1000:
									break
						if attempt < 59:
							print(f"IMPORT : File not ready, checking again in 20 seconds...")
							await asyncio.sleep(20)
					else:
						print(f"IMPORT : File still not ready after 20 minutes")
						return
					# Wait for GBIF to report a stable file size before downloading
					expected_size = 0
					for size_check in range(30):
						async with session.head(url) as size_resp:
							if size_resp.status == 200 and size_resp.headers.get('Content-Length'):
								expected_size = int(size_resp.headers['Content-Length'])
								if expected_size > 1000:
									print(f"IMPORT : GBIF reports {expected_size:,} bytes for {filename}")
									break
						print(f"IMPORT : Waiting for GBIF to finalize file size...")
						await asyncio.sleep(20)
					# Download the file via aria
					success = await aria_download(filename, url, 4, TMP_DIR)
					# Update manifest
					if success:
						manifest['latest_download'] = datetime.now(timezone.utc).isoformat()
						with open(os.path.join(GEO_DIR,'manifest.json'),'w') as f: json.dump(manifest, f, indent=4)
				# Set success if we already had
				else: success = True
				# If we were successful, process incremental update with DuckDB
				if success: process_incremental_update(manifest, filename)

# Start a new GBIF batch job
async def request_update_from_gbif(session, manifest):
	print(f"IMPORT : Requesting latest occurrences from GBIF...")	
	# Get timestamp for latest occurrence we know
	timestamp = manifest.get('latest_download') or manifest.get('initial_download')
	# Start putting together occurrence POST body
	request_body = {
		"creator": settings.GBIF_USER,
		"sendNotification": False,
		"format": "SIMPLE_PARQUET",
		"predicate": {
			"type": "and",
			"predicates": [
				{
						"type": "greaterThan",
						"key": "MODIFIED",
						"value": timestamp[:10]
					},
					{
					"type": "not",
						"predicate": {
							"type": "equals",
							"key": "HAS_GEOSPATIAL_ISSUE",
							"value": True
						}
					},
					{
						"type": "in",
						"key": "KINGDOM_KEY",
						"values": [ 5, 6 ]
					},
					{
						"type": "equals",
						"key": "HAS_COORDINATE",
						"value": True
					}
				]
			}
		}
	# Send it off
	async with session.post(GBIF_API_HOST + 'occurrence/download/request', json=request_body) as resp:
		# Log errors
		if resp.status != 201: 
			print(f"IMPORT : { resp.status } error requesting latest GBIF occurrences: { await resp.text() }")
			# Stop here
			return
		# Remember key
		current_download_key = await resp.text()
		manifest['current_download_key'] = current_download_key
		manifest['latest_download_request'] = datetime.now(timezone.utc).isoformat()
		with open(os.path.join(GEO_DIR,'manifest.json'),'w') as f: json.dump(manifest, f, indent=4)
		# Check response
		print(f"IMPORT : Successfully requested dataset creation { current_download_key } with latest occurrences from GBIF")

# See if GBIF spawned a URL from which we eventually can download the data
async def get_gbif_download_url(session,manifest):
	# Get ID
	pending_id = manifest.get('current_download_key')
	# Sanity
	if not pending_id:
		print(f"IMPORT : ERROR No pending update id found")
		return	
	# Log
	print(f"IMPORT : Fetching pending GBIF occurrence update {pending_id}")
	# Give it 15 minutes
	end_time = datetime.now() + timedelta(minutes=10)
	# Start iterating
	while datetime.now() < end_time:
		try:
			# Send request
			async with session.get(GBIF_API_HOST + 'occurrence/download/request/' + pending_id, allow_redirects=False) as response:
				# If we get a 302 response and update URL
				if response.status == 302 and response.headers.get("Location"): return response.headers.get("Location")
				# Otherwise check if GBIF is trying to tell us something
				elif response.status == 200: print(f"IMPORT : GBIF incremental update status is {await response.text()}")
				# If download was cancelled, is expired etc
				else: 
					# Log
					print(f"IMPORT : GBIF incremental update invalid {await response.text()}")
					# Reset manifest
					manifest.pop('current_download_key', None)
					# Stop here
					return
		except Exception as e: print(f"IMPORT : Error trying to retrieve GBIF incremental update {pending_id}: {e}")
		# Show progress
		(f"IMPORT : Update not yet ready, trying again in 20 seconds...")
		# Wait 20 secs for next request
		time.sleep(20)	

# Shared extraction: iterate parquet chunks in GBIF zip, filter plantae/fungi with valid coords
# Produces temp occurrences table with (id, taxon, location, elevation, spatial_issue)
def extract_from_zip(zip: zipfile.ZipFile, db: duckdb.DuckDBPyConnection):
	# Load spatial and spawn DB outside of file loop
	db.execute("""
		INSTALL spatial;
		LOAD spatial;
		CREATE TEMP TABLE occurrences (id UBIGINT, taxon UINTEGER, location GEOMETRY, elevation SMALLINT, spatial_issue BOOLEAN DEFAULT FALSE);
	""")
	# Logging
	counter = 1
	total = len(zip.filelist)
	# Iterate through all files
	for file in zip.filelist:
		# Ignore empty files
		if file.file_size == 0: continue
		# Read file as Polars dataframe
		df = pl.read_parquet(io.BytesIO(zip.read(file.filename)))
		# Add rows
		db.execute(f"""
			INSERT INTO occurrences BY NAME
			SELECT 
				-- Unique occurrence ID
				gbifid AS id,
				-- We use speciesKey first, as it maps eg synonym occurrences to the correct accepted species
				-- but also fall back on taxonKey, for example if the occurrence is for a genus or family
				COALESCE(speciesKey,taxonkey) AS taxon,
				-- 3 digits gives us about ~71m lon and ~111 meters lat, which is more than enough for 1-10km grids
				-- and 30 arcsecond (~594 meters lon x  ~926 meters lat) lookups
				ST_Point(ROUND(decimallongitude, 3),ROUND(decimallatitude, 3)) AS location,
				COALESCE(elevation,depth * -1) AS elevation,
				-- Add entries with issues as fallback but we filter most in later distillation
				list_contains(issue, 'HAS_GEOSPATIAL_ISSUE') AS spatial_issue
			-- Filter for plants and fungi with valid coordinates, excluding 0/0 null island junk
			FROM df WHERE kingdom IN ('Plantae','Fungi','Incertae sedis') 
			AND decimallatitude IS NOT NULL AND decimallongitude IS NOT NULL
			AND NOT ST_Equals(ST_Point(decimallongitude, decimallatitude), ST_Point(0, 0));
		""")
		# Log, but query only every 10 files
		if counter % 10 == 0 or counter == 1: occurrence_count = db.execute("SELECT COUNT(*) FROM occurrences").fetchone()[0]
		print(f"\rIMPORT : Extracted {occurrence_count:,} occurrences from {counter} of {total} files",end="")
		# Iterate 
		counter += 1
	# Log	
	print(f"\nIMPORT : Successfully extracted {occurrence_count:,} occurrences from {total} files")

# Apply incremental GBIF download: merge new/updated occurrences into existing parquet
def process_incremental_update(manifest,filename):
	# Spawn zipfile and DuckDB
	with zipfile.ZipFile(os.path.join(TMP_DIR, filename), 'r') as zip, duckdb.connect(':memory:') as db:
		# Use shared function to get updates
		extract_from_zip(zip,db)
		# Check if we even have any new occurrences
		new_occurrences = db.execute("SELECT COUNT(*) FROM occurrences").fetchone()[0]
		if not new_occurrences or new_occurrences == 0:
			print(f"IMPORT : No new occurrences in incremental update")		
		# Otherwise process them	
		else:
			db.execute(f"SET temp_directory = '{ TMP_DIR }'")
			# Load existing geoparquet into DuckDB for merge
			existing_occurrence_parquet = db.read_parquet(os.path.join(GEO_DIR,'occurrences.parquet'))
			db.execute(f"""
				INSTALL spatial;
				LOAD spatial;
				CREATE TABLE existing_occurrences AS SELECT * FROM existing_occurrence_parquet;
			""")	
			count = db.execute("SELECT COUNT(*) FROM existing_occurrences").fetchone()[0]
			print(f"IMPORT : Loaded {count:,} occurrences from existing {os.path.join(GEO_DIR,'occurrences.parquet')}")
			# Upsert: update changed occurrences, then insert new ones
			db.execute("""
				UPDATE existing_occurrences
				SET taxon = occurrences.taxon, location = occurrences.location,
					elevation = occurrences.elevation, spatial_issue = occurrences.spatial_issue
				FROM occurrences WHERE existing_occurrences.id = occurrences.id;
			""")
			print(f"IMPORT : Updated existing occurrence data")
			# Insert occurrences not already in existing set
			db.execute("""
				INSERT INTO existing_occurrences SELECT * FROM occurrences AS source
				WHERE NOT EXISTS (SELECT 1 FROM existing_occurrences AS target WHERE target.id = source.id);
			""")
			print(f"IMPORT : Added {db.execute("SELECT COUNT(*) FROM existing_occurrences").fetchone()[0]-count:,} new occurrences")
			# Check data
			if settings.VERBOSE: db.sql("SELECT * FROM occurrences").show(max_rows=200)
			# Write to disc using COPY as write_parquet() doesn't do geoparquet well
			db.sql(f"""COPY existing_occurrences TO '{os.path.join(GEO_DIR, "occurrences.parquet")}';""")
			print(f"IMPORT : Wrote updated occurrences.parquet to {GEO_DIR}")
		# Update manifest
		manifest['last_processed_download_key'] =  manifest.get('current_download_key')
		with open(os.path.join(GEO_DIR,'manifest.json'),'w') as f: json.dump(manifest, f, indent=4)
		print(f"IMPORT : Updated manifest, occurrence update complete")

# Initial bootstrap: extract full GBIF occurrence snapshot (~200GB zip) into geoparquet
def distill_occurrences():		
	print(f"IMPORT : ############### Processing GBIF occurrences ###############")
	# Pointer to zip and spawn db
	with zipfile.ZipFile(os.path.join(TMP_DIR, f"occurrences.zip"), 'r') as zip, duckdb.connect(':memory:') as db:
		# Use shared function
		extract_from_zip(zip,db)
		# Write to disc using COPY as write_parquet() doesn't do geoparquet well
		db.sql(f"""COPY occurrences TO '{os.path.join(GEO_DIR, "occurrences.parquet")}';""")
		print(f"IMPORT : Wrote occurrences.parquet to {GEO_DIR}")

# Load occurrence manifest (tracks download keys and processing state)
def get_manifest() -> dict:
	# Try fetching manifest
	try: 
		with open(os.path.join(GEO_DIR,'manifest.json'), 'r') as f: return json.load(f)
	# Error logging
	except FileNotFoundError: print(f"IMPORT : No GBIF occurrence manifest found")
	except json.JSONDecodeError: print(f"IMPORT : GBIF occurrence manifest corrupted")	
