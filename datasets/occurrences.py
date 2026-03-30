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
import os, io, aiohttp, json, asyncio
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
	# Resolve path to rolling occurrence parquet
	file = os.path.join(GEO_DIR, f"occurrences.parquet")
	# Skip GBIF API update when credentials are missing
	if not settings.GBIF_USER or not settings.GBIF_PASSWORD:
		# Soft-skip when we already have a local occurrence baseline
		if os.path.isfile(file):
			print('IMPORT : GBIF credentials missing, skipping occurrence update and reusing local occurrences.parquet')
			return
		# Hard-stop bootstrap when there is no local baseline at all
		print('IMPORT : GBIF credentials missing, skipping occurrence bootstrap')
		print('IMPORT : Register credentials at https://www.gbif.org/user/profile and set them in canopy/secrets.py')
		raise RuntimeError('No local GBIF occurrences.parquet and no GBIF credentials available')
	# Bootstrap when no processed occurrence parquet exists yet
	if not os.path.isfile(file):
		# Log bootstrap path
		print(f"IMPORT : No processed GBIF occurrence dataset found")
		# Reuse already downloaded bootstrap zip only when it is valid
		bootstrap_zip = os.path.join(TMP_DIR, 'occurrences.zip')
		if os.path.isfile(bootstrap_zip):
			# Ignore stale/corrupt bootstrap zips from interrupted downloads
			if not is_valid_zip(bootstrap_zip):
				print('IMPORT : Existing occurrences.zip is invalid, deleting and re-downloading')
				os.remove(bootstrap_zip)
			# Otherwise process existing bootstrap zip into geoparquet
			else:
				distill_occurrences()
				# Ensure manifest exists after local bootstrap processing
				manifest = get_manifest() or {}
				manifest['initial_download'] = manifest.get('initial_download') or datetime.now(timezone.utc).isoformat()
				save_manifest(manifest)
				return
		# Log API bootstrap request path
		print('IMPORT : Requesting initial GBIF occurrences export via API (plants and fungi only)')
		# Get auth for entire session
		auth = BasicAuth(settings.GBIF_USER, settings.GBIF_PASSWORD)
		# Spawn async http session for bootstrap request polling and download
		async with aiohttp.ClientSession(auth=auth, headers={"User-Agent": user_agent}) as session:
			# Load or initialize occurrence manifest
			manifest = get_manifest() or {}
			# Request bootstrap export when there is no pending key yet
			if not manifest.get('current_download_key'):
				# Start a full initial export limited to required kingdoms and coordinate quality
				await request_update_from_gbif(session, manifest, initial=True)
			# Resolve the pending request key
			pending_key = manifest.get('current_download_key')
			# Hard-fail bootstrap if GBIF did not accept request creation
			if not pending_key: raise RuntimeError('Initial GBIF occurrence export request failed')
			# Resolve ready download URL for pending key
			url = await get_gbif_download_url(session, manifest)
			# Hard-fail bootstrap when export is still being prepared
			if not url: raise RuntimeError('Initial GBIF occurrence export not ready yet please retry in 1 to 2 hours')
			# Download pending initial export with readiness polling
			success = await download_pending_occurrence_file(session, url, 'occurrences.zip', manifest)
			# Hard-fail bootstrap when file is not ready/downloadable yet
			if not success: raise RuntimeError('Initial GBIF occurrence export not ready yet please retry in 1 to 2 hours')
		# Convert bootstrap zip into rolling geoparquet baseline
		distill_occurrences()
		# Persist bootstrap completion in manifest
		manifest = get_manifest() or {}
		manifest['initial_download'] = manifest.get('initial_download') or datetime.now(timezone.utc).isoformat()
		manifest['last_processed_download_key'] = manifest.get('current_download_key')
		save_manifest(manifest)
		# Stop after successful bootstrap
		return
	# Otherwise update existing baseline incrementally
	print(f"IMPORT : Processed GBIF occurrence data found")
	# Run incremental update flow
	await get_latest_occurrences(file)

# Persist manifest JSON to disk in one place
# so all update/bootstrap paths keep state consistently

def save_manifest(manifest: dict):
	# Write manifest atomically from in-memory dict
	with open(os.path.join(GEO_DIR,'manifest.json'),'w') as f: json.dump(manifest, f, indent=4)

# Kick off and wait for an occurrence update
async def get_latest_occurrences(file):
	# Get auth for entire session
	auth = BasicAuth(settings.GBIF_USER, settings.GBIF_PASSWORD)
	# Spawn async http session
	async with aiohttp.ClientSession(auth=auth,headers={"User-Agent": user_agent}) as session:
		# Get current manifest
		manifest = get_manifest()
		# Soft-skip incremental updates if manifest is missing on existing local baseline
		if not manifest:
			print(f"IMPORT : No GBIF occurrence manifest found, skipping incremental update")
			return
		# Check if we have any reason to update, either because we never requested a download yet or it's older than 24h
		if 'latest_download_request' in manifest and datetime.now(timezone.utc) - datetime.fromisoformat(manifest['latest_download_request']) <= timedelta(days=1):
			print(f"IMPORT : Already requested occurrence update { manifest.get('current_download_key') } within the last 24 hours")
		# Otherwise request a new dataset download
		else: await request_update_from_gbif(session, manifest, initial=False)
		# Check if we have a pending update that hasn't been processed yet
		if 'last_processed_download_key' not in manifest or manifest.get('current_download_key') != manifest.get('last_processed_download_key'):
			# Try processing a pending update
			url = await get_gbif_download_url(session, manifest)
			# Soft-skip when GBIF is still preparing incremental export
			if not url:
				print('IMPORT : Incremental GBIF occurrence export not ready yet, will retry in next run')
				return
			# Set a filename for pending incremental export
			filename = f"occurrence_update.{ manifest.get('current_download_key') }.zip"
			# Download with readiness checks (or reuse existing file)
			success = await download_pending_occurrence_file(session, url, filename, manifest)
			# Soft-skip when file is still not ready
			if not success:
				print('IMPORT : Incremental GBIF occurrence export file not ready yet, will retry in next run')
				return
			# Process successful incremental zip into rolling geoparquet
			process_incremental_update(manifest, filename)

# Start a new GBIF batch job
async def request_update_from_gbif(session, manifest, initial=False):
	# Log request mode
	print(f"IMPORT : Requesting {'initial' if initial else 'latest'} occurrences from GBIF...")
	# Build shared predicate base for both initial and incremental runs
	predicates = [
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
			"values": [0, 5, 6]
		},
		{
			"type": "equals",
			"key": "HAS_COORDINATE",
			"value": True
		},
		{
			"type": "equals",
			"key": "OCCURRENCE_STATUS",
			"value": "PRESENT"
		}
	]
	# Add MODIFIED cutoff only for incremental updates
	if not initial:
		# Get timestamp for latest occurrence we know
		timestamp = manifest.get('latest_download') or manifest.get('initial_download')
		# Skip incremental request when no baseline timestamp exists
		if not timestamp:
			print('IMPORT : No occurrence baseline timestamp in manifest, skipping incremental request')
			return
		# Prepend modified cutoff for incremental delta request
		predicates.insert(0, {
			"type": "greaterThan",
			"key": "MODIFIED",
			"value": timestamp[:10]
		})
	# Build request body
	request_body = {
		"creator": settings.GBIF_USER,
		"sendNotification": False,
		"format": "SIMPLE_PARQUET",
		"predicate": {
			"type": "and",
			"predicates": predicates
		}
	}
	# Send request to GBIF async download endpoint
	async with session.post(GBIF_API_HOST + 'occurrence/download/request', json=request_body) as resp:
		# Log and stop when GBIF rejects request creation
		if resp.status != 201:
			print(f"IMPORT : { resp.status } error requesting {'initial' if initial else 'latest'} GBIF occurrences: { await resp.text() }")
			return
		# Remember pending request key
		current_download_key = await resp.text()
		manifest['current_download_key'] = current_download_key
		manifest['latest_download_request'] = datetime.now(timezone.utc).isoformat()
		save_manifest(manifest)
		# Log success
		print(f"IMPORT : Successfully requested dataset creation { current_download_key } with {'initial' if initial else 'latest'} occurrences from GBIF")

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
		print(f"IMPORT : Update not yet ready, trying again in 20 seconds...")
		# Wait 20 secs for next request
		await asyncio.sleep(20)

# Check zip integrity quickly before processing
# Returns True when zip central directory can be read, False otherwise

def is_valid_zip(path: str) -> bool:
	# Sanity check file presence
	if not os.path.isfile(path): return False
	# Validate zip central directory
	try:
		with zipfile.ZipFile(path, 'r') as z: z.namelist()
		return True
	except Exception: return False

# Wait for GBIF file readiness and download with aria
# Returns True when file exists and is valid, False when still not ready/failed
async def download_pending_occurrence_file(session, url, filename, manifest):
	# Resolve local file path once
	filepath = os.path.join(TMP_DIR, filename)
	# Reuse previously downloaded file only if zip is valid
	if is_valid_zip(filepath): return True
	# Remove stale/corrupt partial zip before retrying download
	if os.path.isfile(filepath): os.remove(filepath)
	# Wait up to 20 minutes for remote file availability
	for attempt in range(60):
		# Probe download URL headers
		async with session.head(url) as head_resp:
			# Check if GBIF now serves a real file with length
			if head_resp.status == 200:
				content_length = head_resp.headers.get('Content-Length')
				if content_length and int(content_length) > 1000: break
		# Wait unless this was final retry
		if attempt < 59:
			print(f"IMPORT : File not ready, checking again in 20 seconds...")
			await asyncio.sleep(20)
	# Stop when file was never ready within wait window
	else:
		print(f"IMPORT : File still not ready after 20 minutes")
		return False
	# Wait for GBIF to report a stable file size before downloading
	expected_size = 0
	verify_after_delay = False
	for size_check in range(30):
		# Probe current content length
		async with session.head(url) as size_resp:
			# Read current file size from GBIF headers
			if size_resp.status == 200 and size_resp.headers.get('Content-Length'):
				current_size = int(size_resp.headers['Content-Length'])
			else: current_size = 0
		# Skip invalid/empty size responses
		if current_size <= 1000:
			print(f"IMPORT : GBIF still adding to zip (currently {current_size / (1024**3):.1f}GB), retrying in 20 secs...")
			await asyncio.sleep(20)
			continue
		# Confirm delayed verification check if we previously saw matching sizes
		if verify_after_delay:
			# Proceed only if size remained unchanged after the longer wait
			if current_size == expected_size:
				print(f"IMPORT : GBIF reports stable size {expected_size:,} bytes for {filename}")
				break
			# Reset verification state when size changed again
			verify_after_delay = False
			expected_size = current_size
			print(f"IMPORT : GBIF still adding to zip (currently {current_size / (1024**3):.1f}GB), retrying in 20 secs...")
			await asyncio.sleep(20)
			continue
		# First matching check: pause briefly before final confirmation
		if current_size == expected_size:
			print(f"IMPORT : GBIF zip looks complete, waiting 20 secs to verify...")
			verify_after_delay = True
			await asyncio.sleep(20)
			continue
		# Track latest size and keep polling
		expected_size = current_size
		print(f"IMPORT : GBIF still adding to zip (currently {current_size / (1024**3):.1f}GB), retrying in 20 secs...")
		await asyncio.sleep(20)
	# Download the file via aria
	success = await aria_download(filename, url, 4, TMP_DIR)
	# Treat invalid/corrupt zip as unsuccessful download
	if success and not is_valid_zip(filepath):
		print('IMPORT : Download completed but zip validation failed, will retry later')
		return False
	# Persist latest successful download timestamp
	if success:
		manifest['latest_download'] = datetime.now(timezone.utc).isoformat()
		save_manifest(manifest)
	# Return download outcome
	return bool(success)

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
