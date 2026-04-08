# Load async helper for running blocking boto3 calls without blocking event loop
from .log import mesologger
import asyncio
# Load JSON helpers for manifest read/write abstraction
import json
# Load filesystem helpers for local storage backend and temp downloads
import os
# Load SSL context for parity with existing downloader behavior
import ssl
# Load timing helpers for periodic multipart progress logging
import time
# Load canopy paths and settings proxy
from .. import DATA_DIR, settings

# Cache the active storage backend so callers can import a stable proxy
_storage_cache = None
# Cache signature of active config so we rebuild backend when settings change
_storage_signature = None
# Track whether missing-S3-config warning was already emitted
_storage_warned_missing_config = False

# Local filesystem storage backend preserving current canopy behavior
class LocalStorage:
	# Return files directly under a directory with optional prefix/suffix filtering
	def list_files(self, dir, prefix=None, suffix=None):
		# Return empty list when directory is missing
		if not os.path.isdir(dir): return []
		# Build filtered file list from local directory entries
		return [
			file for file in os.listdir(dir)
			# Keep only regular files
			if os.path.isfile(os.path.join(dir, file))
			# Apply optional starts-with filter
			and (prefix is None or file.startswith(prefix))
			# Apply optional ends-with filter
			and (suffix is None or file.endswith(suffix))
		]

	# Return directory names directly under a directory
	def list_dirs(self, dir):
		# Return empty list when directory is missing
		if not os.path.isdir(dir): return []
		# Build subdirectory list from local entries
		return [
			entry for entry in os.listdir(dir)
			# Keep only direct child directories
			if os.path.isdir(os.path.join(dir, entry))
		]

	# Check local file existence
	def exists(self, path):
		# Return true only for existing regular files
		return os.path.isfile(path)

	# Return local file size in bytes
	def size(self, path):
		# Return file size when file exists
		if os.path.isfile(path): return os.path.getsize(path)
		# Return 0 for missing files
		return 0

	# Delete a local file if present
	def delete(self, path):
		# Skip silently when file is already missing
		if not os.path.isfile(path): return
		# Remove local file
		os.remove(path)

	# Read JSON payload from local file path
	def read_json(self, path):
		# Open JSON file and decode payload
		with open(path, 'r', encoding='utf-8') as file: return json.load(file)

	# Write JSON payload to local file path
	def write_json(self, path, data):
		# Ensure parent directory exists before writing JSON
		os.makedirs(os.path.dirname(path), exist_ok=True)
		# Serialize JSON payload with deterministic indentation
		with open(path, 'w', encoding='utf-8') as file: json.dump(data, file, indent=4)

	# Create local directory tree when needed
	def makedirs(self, path):
		# Create directory tree idempotently
		os.makedirs(path, exist_ok=True)

	# Copy local file to destination path
	def copy(self, src, dst):
		# Load shutil lazily to keep module imports minimal
		import shutil
		# Ensure destination directory exists before copy
		os.makedirs(os.path.dirname(dst), exist_ok=True)
		# Copy file preserving metadata
		shutil.copy2(src, dst)

	# Write raw bytes payload to storage path
	def write_bytes(self, path, data, content_type='application/octet-stream'):
		# Ensure destination directory exists before writing bytes
		os.makedirs(os.path.dirname(path), exist_ok=True)
		# Write payload bytes to local filesystem
		with open(path, 'wb') as file: file.write(data)

	# Copy one local file into storage path
	def put_file(self, path, local_path):
		# Load shutil lazily to keep module imports minimal
		import shutil
		# Ensure destination directory exists before copy
		os.makedirs(os.path.dirname(path), exist_ok=True)
		# Copy local file into destination path
		shutil.copy2(local_path, path)

	# Remove local directory tree recursively
	def rmtree(self, path):
		# Load shutil lazily to keep module imports minimal
		import shutil
		# Skip silently when directory is missing
		if not os.path.isdir(path): return
		# Remove directory tree recursively
		shutil.rmtree(path)

	# Signal local backend mode
	def is_s3(self):
		# Local mode is not S3
		return False

	# Ensure local path exists and return it (identity in local mode)
	def ensure_local(self, path, local_dir=None):
		# Local mode already uses filesystem paths directly
		return path

	# Return parquet URL/path usable by DuckDB
	def parquet_url(self, path):
		# DuckDB reads local paths directly
		return path.replace('\\', '/')

	# Configure DuckDB for S3 access (no-op in local mode)
	def configure_duckdb(self, db):
		# Local mode needs no extra DuckDB configuration
		return


# S3-compatible storage backend using boto3
class S3Storage:
	# Build S3 backend from resolved runtime settings
	def __init__(self, bucket, endpoint, access_key, secret_key, region):
		# Store target bucket used by canopy data paths
		self.bucket = bucket
		# Store endpoint as provided for boto3 endpoint_url
		self.endpoint = endpoint
		# Store region for boto3 and DuckDB configuration
		self.region = region
		# Store access key for DuckDB runtime configuration
		self.access_key = access_key
		# Store secret key for DuckDB runtime configuration
		self.secret_key = secret_key
		# Cache endpoint host without scheme for DuckDB s3_endpoint
		self.endpoint_host = endpoint.replace('https://', '').replace('http://', '')
		# Track endpoint TLS mode for DuckDB config
		self.use_ssl = endpoint.startswith('https://')
		# Import boto3 lazily so local mode does not require boto3 dependency
		import boto3
		# Import botocore client config lazily with boto3
		from botocore.client import Config
		# Import botocore client error type for S3 error handling
		from botocore.exceptions import ClientError
		# Keep client error class on instance for later exception checks
		self.client_error = ClientError
		# Build boto3 S3 client with Hetzner-recommended settings
		self.s3 = boto3.client(
			# Use S3 service client
			's3',
			# Use configured region
			region_name=region,
			# Use configured S3-compatible endpoint
			endpoint_url=endpoint,
			# Inject access key from canopy settings/env
			aws_access_key_id=access_key,
			# Inject secret key from canopy settings/env
			aws_secret_access_key=secret_key,
			# Apply Hetzner-compatible signature and URL style settings
			config=Config(
				# Force Signature V4
				signature_version='s3v4',
				# Match Hetzner guidance for payload signing and virtual host style
				s3={'payload_signing_enabled': False, 'addressing_style': 'virtual'},
			),
		)

	# Convert local canopy path into relative S3 object key
	def _key_from_path(self, path):
		# Normalize path separators for cross-platform consistency
		normalized = os.path.normpath(path)
		# Normalize DATA_DIR for prefix checks
		data_norm = os.path.normpath(DATA_DIR)
		# If path is inside DATA_DIR, strip DATA_DIR prefix
		if os.path.commonpath([data_norm, normalized]) == data_norm:
			# Build relative path from canopy data root
			rel = os.path.relpath(normalized, data_norm)
		# Otherwise treat incoming path as already-relative key
		else: rel = normalized
		# Convert to POSIX separators expected by S3 APIs
		return rel.replace('\\', '/').lstrip('/')

	# Convert local canopy directory path into S3 prefix ending with slash
	def _dir_prefix(self, dir):
		# Resolve relative key for the directory
		key = self._key_from_path(dir)
		# Ensure prefixes end with slash for proper scoping
		if key and not key.endswith('/'): key += '/'
		# Return normalized prefix
		return key

	# Build destination key from optional override or local path
	def _resolve_key(self, path_or_key):
		# Preserve explicit S3-style keys passed by callers
		if '/' in path_or_key and not os.path.isabs(path_or_key) and not path_or_key.startswith(DATA_DIR): return path_or_key.replace('\\', '/')
		# Otherwise derive key from canopy local path
		return self._key_from_path(path_or_key)

	# List direct files in one logical canopy directory prefix
	def list_files(self, dir, prefix=None, suffix=None):
		# Build scoped directory prefix from canopy path
		dir_prefix = self._dir_prefix(dir)
		# Build object prefix using optional filename prefix filter
		object_prefix = dir_prefix + (prefix or '')
		# Track returned direct child filenames
		files = []
		# Track pagination token for long listings
		continuation = None
		# Iterate until S3 signals there are no more pages
		while True:
			# Build list request parameters
			params = {
				# Target bucket
				'Bucket': self.bucket,
				# Prefix-scoped listing
				'Prefix': object_prefix,
			}
			# Attach continuation token when paginating
			if continuation: params['ContinuationToken'] = continuation
			# Execute list request
			response = self.s3.list_objects_v2(**params)
			# Iterate listed objects when page has contents
			for entry in response.get('Contents', []):
				# Resolve key relative to logical directory prefix
				rel = entry['Key'][len(dir_prefix):] if entry['Key'].startswith(dir_prefix) else entry['Key']
				# Skip nested paths when caller expects direct child files
				if '/' in rel: continue
				# Apply optional suffix filter
				if suffix and not rel.endswith(suffix): continue
				# Add filename to result set
				files.append(rel)
			# Continue pagination while more pages are available
			if response.get('IsTruncated'):
				# Store continuation token for next page
				continuation = response.get('NextContinuationToken')
			# Stop pagination when listing is complete
			else: break
		# Return filtered filenames
		return files

	# List direct child directory names for a canopy prefix
	def list_dirs(self, dir):
		# Build scoped directory prefix from canopy path
		dir_prefix = self._dir_prefix(dir)
		# Track returned directory names
		dirs = []
		# Track pagination token for long listings
		continuation = None
		# Iterate until S3 signals there are no more pages
		while True:
			# Build list request with delimiter for directory emulation
			params = {
				# Target bucket
				'Bucket': self.bucket,
				# Prefix-scoped listing
				'Prefix': dir_prefix,
				# Request only first path segment beyond prefix
				'Delimiter': '/',
			}
			# Attach continuation token when paginating
			if continuation: params['ContinuationToken'] = continuation
			# Execute list request
			response = self.s3.list_objects_v2(**params)
			# Collect common-prefix directory names
			for common in response.get('CommonPrefixes', []):
				# Strip parent prefix and trailing slash for folder name
				dirs.append(common['Prefix'][len(dir_prefix):].rstrip('/'))
			# Continue pagination while more pages are available
			if response.get('IsTruncated'):
				# Store continuation token for next page
				continuation = response.get('NextContinuationToken')
			# Stop pagination when listing is complete
			else: break
		# Return directory names
		return dirs

	# Check object existence via HEAD request
	def exists(self, path):
		# Resolve object key from path-like input
		key = self._resolve_key(path)
		try:
			# Request object metadata
			self.s3.head_object(Bucket=self.bucket, Key=key)
			# Return true when object exists
			return True
		except self.client_error as err:
			# Return false for not-found responses
			if err.response.get('ResponseMetadata', {}).get('HTTPStatusCode') == 404: return False
			# Return false for S3 NoSuchKey responses
			if err.response.get('Error', {}).get('Code') in ['404', 'NoSuchKey', 'NotFound']: return False
			# Re-raise unexpected API errors
			raise

	# Return object size in bytes via HEAD metadata
	def size(self, path):
		# Resolve object key from path-like input
		key = self._resolve_key(path)
		try:
			# Fetch object metadata
			response = self.s3.head_object(Bucket=self.bucket, Key=key)
			# Return content length
			return int(response.get('ContentLength', 0))
		except self.client_error:
			# Return 0 when object is missing
			return 0

	# Delete one object key if present
	def delete(self, path):
		# Resolve object key from path-like input
		key = self._resolve_key(path)
		# Delete object idempotently
		self.s3.delete_object(Bucket=self.bucket, Key=key)

	# Read and decode JSON object from S3
	def read_json(self, path):
		# Resolve object key from path-like input
		key = self._resolve_key(path)
		try:
			# Fetch object payload from S3
			response = self.s3.get_object(Bucket=self.bucket, Key=key)
		except self.client_error as err:
			# Map missing-object responses to FileNotFoundError for storage parity
			if err.response.get('ResponseMetadata', {}).get('HTTPStatusCode') == 404: raise FileNotFoundError(path)
			# Map S3 NoSuchKey/NotFound style codes to FileNotFoundError
			if err.response.get('Error', {}).get('Code') in ['404', 'NoSuchKey', 'NotFound']: raise FileNotFoundError(path)
			# Re-raise unexpected API errors
			raise
		# Decode JSON payload bytes as UTF-8
		payload = response['Body'].read().decode('utf-8')
		# Return parsed JSON data
		return json.loads(payload)

	# Serialize and upload JSON object to S3
	def write_json(self, path, data):
		# Resolve object key from path-like input
		key = self._resolve_key(path)
		# Serialize payload with deterministic formatting
		payload = json.dumps(data, indent=4).encode('utf-8')
		# Upload JSON payload to S3
		self.s3.put_object(Bucket=self.bucket, Key=key, Body=payload, ContentType='application/json')

	# Write raw bytes payload to S3 object key
	def write_bytes(self, path, data, content_type='application/octet-stream'):
		# Resolve object key from path-like input
		key = self._resolve_key(path)
		# Upload raw bytes payload to S3 object storage
		self.s3.put_object(Bucket=self.bucket, Key=key, Body=data, ContentType=content_type)

	# Copy one local file into a storage path key
	def put_file(self, path, local_path):
		# Resolve object key from path-like input
		key = self._resolve_key(path)
		# Upload local file into resolved object key
		self.s3.upload_file(local_path, self.bucket, key)

	# Directory creation is a no-op on object storage
	def makedirs(self, path):
		# S3 has no real directories
		return

	# Copy one object into another key with fallback when CopyObject fails
	def copy(self, src, dst):
		# Resolve source key from path-like input
		src_key = self._resolve_key(src)
		# Resolve destination key from path-like input
		dst_key = self._resolve_key(dst)
		try:
			# Try direct server-side copy first for efficiency
			self.s3.copy_object(Bucket=self.bucket, CopySource={'Bucket': self.bucket, 'Key': src_key}, Key=dst_key)
		except Exception:
			# Fall back to download-upload path for backends with flaky CopyObject
			response = self.s3.get_object(Bucket=self.bucket, Key=src_key)
			# Upload source body stream into destination object
			self.s3.upload_fileobj(response['Body'], self.bucket, dst_key)

	# Remove all objects under one logical prefix recursively
	def rmtree(self, path):
		# Resolve prefix key from path-like input
		prefix = self._dir_prefix(path)
		# Track pagination token for long listings
		continuation = None
		# Iterate object pages until prefix is exhausted
		while True:
			# Build list request parameters
			params = {'Bucket': self.bucket, 'Prefix': prefix}
			# Attach continuation token when paginating
			if continuation: params['ContinuationToken'] = continuation
			# Fetch one page of objects under prefix
			response = self.s3.list_objects_v2(**params)
			# Build delete batch payload for this page
			objects = [{'Key': item['Key']} for item in response.get('Contents', [])]
			# Delete objects in batch when page has keys
			if objects: self.s3.delete_objects(Bucket=self.bucket, Delete={'Objects': objects})
			# Continue pagination while more pages are available
			if response.get('IsTruncated'):
				# Store continuation token for next page
				continuation = response.get('NextContinuationToken')
			# Stop pagination when listing is complete
			else: break

	# Signal S3 backend mode
	def is_s3(self):
		# Backend is S3-compatible object storage
		return True

	# Ensure an S3 object is present on local disk and return local path
	def ensure_local(self, path, local_dir=None):
		# Resolve source key from path-like input
		key = self._resolve_key(path)
		# Build default local path from original path
		local_path = path
		# Override local path to chosen local directory when requested
		if local_dir: local_path = os.path.join(local_dir, os.path.basename(path))
		# Ensure destination directory exists before downloading
		os.makedirs(os.path.dirname(local_path), exist_ok=True)
		# Skip download when local file already exists and has data
		if os.path.isfile(local_path) and os.path.getsize(local_path) > 0: return local_path
		# Download object to local path
		self.s3.download_file(self.bucket, key, local_path)
		# Return ensured local file path
		return local_path

	# Build DuckDB-compatible s3:// URL for a canopy path
	def parquet_url(self, path):
		# Resolve object key from path-like input
		key = self._resolve_key(path)
		# Return canonical S3 URL
		return f's3://{self.bucket}/{key}'

	# Upload one local file into S3 under optional key override
	def upload(self, local_path, remote_key=None):
		# Resolve destination key from override or local path
		key = remote_key.replace('\\', '/') if remote_key else self._resolve_key(local_path)
		# Upload file from local disk to object storage
		self.s3.upload_file(local_path, self.bucket, key)

	# Download one S3 key to local path
	def download(self, remote_key, local_path):
		# Ensure destination directory exists
		os.makedirs(os.path.dirname(local_path), exist_ok=True)
		# Download object into local file
		self.s3.download_file(self.bucket, remote_key.replace('\\', '/'), local_path)

	# Configure DuckDB httpfs for S3 reads/writes
	def configure_duckdb(self, db):
		# Load httpfs extension for S3 access
		db.execute('INSTALL httpfs; LOAD httpfs;')
		# Configure access key for S3 requests
		db.execute(f"SET s3_access_key_id = '{self.access_key}';")
		# Configure secret key for S3 requests
		db.execute(f"SET s3_secret_access_key = '{self.secret_key}';")
		# Configure region for request signing
		db.execute(f"SET s3_region = '{self.region}';")
		# Configure endpoint host for non-AWS S3 backends
		db.execute(f"SET s3_endpoint = '{self.endpoint_host}';")
		# Match Hetzner virtual-hosted bucket style
		db.execute("SET s3_url_style = 'vhost';")
		# Keep TLS on for https endpoints
		db.execute(f"SET s3_use_ssl = {'true' if self.use_ssl else 'false'};")

	# Stream remote HTTP response directly into S3 multipart upload with ranged parallel workers
	async def stream_to_s3_parallel(self, url, key, session, parallel):
		# Normalize remote key for S3 API calls
		object_key = key.replace('\\', '/')
		# Clamp worker count to a sane minimum
		worker_count = max(1, int(parallel or 1))
		# Pick target multipart part size to keep part count manageable
		target_part_size = 64 * 1024 * 1024
		# Log every 512MB to keep journals concise for very large files
		log_step_bytes = 512 * 1024 * 1024
		# Resolve source metadata for ranged download eligibility
		content_length = 0
		# Track whether remote server advertises byte-range support
		supports_ranges = False
		# Probe remote headers before starting multipart upload
		try:
			# Request response headers for content length and range support
			async with session.head(url, ssl=ssl.create_default_context()) as head_response:
				# Raise when HEAD request failed
				head_response.raise_for_status()
				# Parse remote content length for fixed range partitioning
				content_length = int(head_response.headers.get('Content-Length', '0') or 0)
				# Parse Accept-Ranges header and detect byte-range support
				supports_ranges = 'bytes' in str(head_response.headers.get('Accept-Ranges', '')).lower()
		# Fall back to single-stream mode when HEAD probe fails
		except Exception:
			# Keep fallback values so caller uses non-ranged stream path
			content_length = 0
			# Mark range support unavailable when probe failed
			supports_ranges = False
		# Fall back to single-stream upload when range mode is not viable
		if worker_count <= 1 or content_length <= 0 or not supports_ranges:
			# Use buffered single-stream multipart uploader as compatibility path
			await self.stream_to_s3(url, key, session)
			# Return after compatibility fallback
			return
		# Compute number of multipart parts from full content length
		part_count = (content_length + target_part_size - 1) // target_part_size
		# Track uploaded part descriptors by part number
		part_etags = {}
		# Track uploaded payload bytes for progress logging
		total_uploaded_bytes = 0
		# Track next byte threshold for emitting progress logs
		next_log_threshold = log_step_bytes
		# Track start time for average throughput calculations
		start_time = time.monotonic()
		# Track next unassigned part number across worker coroutines
		next_part_number = 1
		# Protect part assignment and progress counters between workers
		assignment_lock = asyncio.Lock()
		# Protect progress counters and etag map updates between workers
		progress_lock = asyncio.Lock()
		# Start multipart upload and capture upload id
		create = await asyncio.to_thread(self.s3.create_multipart_upload, Bucket=self.bucket, Key=object_key)
		upload_id = create['UploadId']
		# Log start of ranged multipart upload mode
		mesologger.info(f"Streaming {url} to s3://{self.bucket}/{object_key} using {worker_count} ranged workers")
		# Worker coroutine uploads one or more assigned parts
		async def worker(worker_index):
			# Stagger worker startup to reduce initial burst throttling on range requests
			if worker_index > 0: await asyncio.sleep(worker_index * 2.5)
			# Access shared assignment and progress state from outer scope
			nonlocal next_part_number, total_uploaded_bytes, next_log_threshold
			# Keep consuming parts until all ranges were uploaded
			while True:
				# Assign next part number atomically across workers
				async with assignment_lock:
					# Stop worker when all parts were already assigned
					if next_part_number > part_count: return
					# Reserve one part number for this worker iteration
					part_number = next_part_number
					# Increment shared part cursor for next assignment
					next_part_number += 1
				# Compute inclusive byte range for this part number
				byte_start = (part_number - 1) * target_part_size
				# Compute inclusive byte end capped to final payload byte
				byte_end = min(content_length - 1, byte_start + target_part_size - 1)
				# Build expected payload length for integrity checks
				expected_bytes = byte_end - byte_start + 1
				# Build HTTP range header for this part
				range_header = {'Range': f'bytes={byte_start}-{byte_end}'}
				# Retry transient range/download/upload failures per part
				for attempt in range(5):
					# Track whether this part hit repeated upstream 503 responses
					had_503 = False
					try:
						# Fetch one ranged payload segment from upstream source
						async with session.get(url, headers=range_header, ssl=ssl.create_default_context()) as response:
							# Flag transient upstream throttling for targeted backoff and fallback
							if response.status == 503:
								had_503 = True
								raise RuntimeError(f'Unexpected range status {response.status} for part {part_number}')
							# Abort on other unexpected response codes
							if response.status not in [200, 206]: raise RuntimeError(f'Unexpected range status {response.status} for part {part_number}')
							# Guard against full-file responses on non-first parts
							if response.status == 200 and (byte_start != 0 or byte_end != content_length - 1): raise RuntimeError(f'Server ignored range for part {part_number}')
							# Read entire part payload into memory for upload
							payload = await response.read()
						# Guard against short or oversized part payloads
						if len(payload) != expected_bytes: raise RuntimeError(f'Range payload size mismatch for part {part_number} expected {expected_bytes} got {len(payload)}')
						# Upload part payload to S3 multipart upload
						upload = await asyncio.to_thread(
							self.s3.upload_part,
							Bucket=self.bucket,
							Key=object_key,
							UploadId=upload_id,
							PartNumber=part_number,
							Body=payload,
						)
						# Commit successful part metadata and progress counters
						async with progress_lock:
							# Store ETag by part number for ordered completion payload
							part_etags[part_number] = upload['ETag']
							# Add uploaded byte count to transfer progress total
							total_uploaded_bytes += len(payload)
							# Emit periodic throughput logs on configured intervals
							if total_uploaded_bytes >= next_log_threshold:
								# Compute elapsed seconds for throughput reporting
								elapsed = max(time.monotonic() - start_time, 1e-6)
								# Compute average MB/s since start of transfer
								rate_mbps = (total_uploaded_bytes / (1024 * 1024)) / elapsed
								# Log progress with uploaded bytes and committed part count
								mesologger.info(f"Streamed {total_uploaded_bytes // (1024 * 1024)}MB to s3://{self.bucket}/{object_key} in {len(part_etags)} parts avg {rate_mbps:.2f}MB/s")
								# Advance next progress threshold until above current byte count
								while total_uploaded_bytes >= next_log_threshold: next_log_threshold += log_step_bytes
						# Stop retry loop after successful part upload
						break
					# Retry with a short backoff when this attempt failed
					except Exception:
						# Fall back to one-off single-stream range fetch after repeated 503 throttling
						if had_503 and attempt == 4:
							# Log fallback path for operator visibility
							mesologger.warning(f"Falling back to single-range fetch for part {part_number} after repeated 503 responses")
							# Fetch this exact range without worker competition and upload once
							async with session.get(url, headers=range_header, ssl=ssl.create_default_context()) as response:
								# Raise when fallback response is still not usable
								if response.status not in [200, 206]: raise RuntimeError(f'Fallback range status {response.status} for part {part_number}')
								# Read fallback payload bytes
								payload = await response.read()
							# Guard fallback payload size for integrity
							if len(payload) != expected_bytes: raise RuntimeError(f'Fallback range payload size mismatch for part {part_number} expected {expected_bytes} got {len(payload)}')
							# Upload fallback payload part
							upload = await asyncio.to_thread(
								self.s3.upload_part,
								Bucket=self.bucket,
								Key=object_key,
								UploadId=upload_id,
								PartNumber=part_number,
								Body=payload,
							)
							# Commit fallback part progress under lock
							async with progress_lock:
								# Store fallback ETag for ordered completion
								part_etags[part_number] = upload['ETag']
								# Add fallback payload size to progress counters
								total_uploaded_bytes += len(payload)
								# Emit progress logs when threshold crossed
								if total_uploaded_bytes >= next_log_threshold:
									# Compute elapsed seconds for throughput reporting
									elapsed = max(time.monotonic() - start_time, 1e-6)
									# Compute average MB/s since start of transfer
									rate_mbps = (total_uploaded_bytes / (1024 * 1024)) / elapsed
									# Log fallback progress and throughput summary
									mesologger.info(f"Streamed {total_uploaded_bytes // (1024 * 1024)}MB to s3://{self.bucket}/{object_key} in {len(part_etags)} parts avg {rate_mbps:.2f}MB/s")
									# Advance next log threshold above current byte count
									while total_uploaded_bytes >= next_log_threshold: next_log_threshold += log_step_bytes
							# Stop retry loop after successful fallback upload
							break
						# Raise on final failed attempt to abort whole multipart run
						if attempt == 4: raise
						# Sleep before retrying this part (longer when upstream returns 503)
						if had_503: await asyncio.sleep((2 ** attempt) + 0.5)
						# Keep short retry cadence for non-503 transient errors
						else: await asyncio.sleep(1 + attempt)
		try:
			# Run all workers concurrently to fetch and upload part ranges
			await asyncio.gather(*[worker(index) for index in range(worker_count)])
			# Build ordered part descriptor list for multipart completion
			parts = [{'PartNumber': number, 'ETag': part_etags[number]} for number in sorted(part_etags.keys())]
			# Complete multipart upload after all parts succeeded
			await asyncio.to_thread(
				self.s3.complete_multipart_upload,
				Bucket=self.bucket,
				Key=object_key,
				UploadId=upload_id,
				MultipartUpload={'Parts': parts},
			)
			# Compute elapsed seconds for final completion summary
			elapsed = max(time.monotonic() - start_time, 1e-6)
			# Compute final average MB/s for completion summary
			rate_mbps = (total_uploaded_bytes / (1024 * 1024)) / elapsed
			# Log final streamed upload completion summary
			mesologger.info(f"Stream upload complete {total_uploaded_bytes // (1024 * 1024)}MB to s3://{self.bucket}/{object_key} in {len(parts)} parts avg {rate_mbps:.2f}MB/s")
		except Exception:
			# Abort multipart upload so partial objects are not left behind
			await asyncio.to_thread(self.s3.abort_multipart_upload, Bucket=self.bucket, Key=object_key, UploadId=upload_id)
			# Re-raise original failure for caller handling
			raise

	# Stream remote HTTP response directly into S3 multipart upload
	async def stream_to_s3(self, url, key, session):
		# Normalize remote key for S3 API calls
		object_key = key.replace('\\', '/')
		# Pick target multipart part size to avoid tiny-fragment uploads
		target_part_size = 64 * 1024 * 1024
		# Read source stream in smaller chunks and aggregate into target-sized parts
		read_chunk_size = 8 * 1024 * 1024
		# Log every 512MB to keep journals concise for very large files
		log_step_bytes = 512 * 1024 * 1024
		# Track uploaded part descriptors for completion
		parts = []
		# Track uploaded payload bytes for progress logging
		total_uploaded_bytes = 0
		# Track next byte threshold for emitting progress logs
		next_log_threshold = log_step_bytes
		# Track start time for average throughput calculations
		start_time = time.monotonic()
		# Start multipart upload and capture upload id
		create = await asyncio.to_thread(self.s3.create_multipart_upload, Bucket=self.bucket, Key=object_key)
		upload_id = create['UploadId']
		try:
			# Open streaming HTTP request for source payload
			async with session.get(url, ssl=ssl.create_default_context()) as response:
				# Raise for HTTP failures before uploading parts
				response.raise_for_status()
				# Log start of streamed multipart upload
				mesologger.info(f"Streaming {url} to s3://{self.bucket}/{object_key}")
				# Start part numbering at 1 per S3 API contract
				part_number = 1
				# Keep in-memory buffer until one full multipart part is ready
				buffer = bytearray()
				# Stream source payload in read chunks and fill part buffer
				async for chunk in response.content.iter_chunked(read_chunk_size):
					# Skip empty chunks from keepalive boundaries
					if not chunk: continue
					# Append read chunk to multipart buffer
					buffer.extend(chunk)
					# Upload as many full parts as available in buffer
					while len(buffer) >= target_part_size:
						# Slice one full multipart part payload from buffer
						part_payload = bytes(buffer[:target_part_size])
						# Remove uploaded bytes from front of buffer
						del buffer[:target_part_size]
						# Upload one multipart segment using blocking boto3 in thread
						upload = await asyncio.to_thread(
							self.s3.upload_part,
							Bucket=self.bucket,
							Key=object_key,
							UploadId=upload_id,
							PartNumber=part_number,
							Body=part_payload,
						)
						# Record uploaded part metadata for completion
						parts.append({'PartNumber': part_number, 'ETag': upload['ETag']})
						# Add current part length to uploaded byte counter
						total_uploaded_bytes += len(part_payload)
						# Emit periodic progress logs when threshold was reached
						if total_uploaded_bytes >= next_log_threshold:
							# Compute elapsed seconds for throughput reporting
							elapsed = max(time.monotonic() - start_time, 1e-6)
							# Compute average MB/s since start of transfer
							rate_mbps = (total_uploaded_bytes / (1024 * 1024)) / elapsed
							# Log streamed upload progress with part counter and throughput
							mesologger.info(f"Streamed {total_uploaded_bytes // (1024 * 1024)}MB to s3://{self.bucket}/{object_key} in {len(parts)} parts avg {rate_mbps:.2f}MB/s")
							# Move next progress threshold forward by one logging window
							next_log_threshold += log_step_bytes
						# Increment part number for next upload call
						part_number += 1
				# Upload final buffered tail as last multipart part
				if len(buffer) > 0:
					# Freeze remaining bytes for final part upload
					part_payload = bytes(buffer)
					# Upload trailing part payload
					upload = await asyncio.to_thread(
						self.s3.upload_part,
						Bucket=self.bucket,
						Key=object_key,
						UploadId=upload_id,
						PartNumber=part_number,
						Body=part_payload,
					)
					# Record uploaded final part metadata for completion
					parts.append({'PartNumber': part_number, 'ETag': upload['ETag']})
					# Add trailing bytes to uploaded byte counter
					total_uploaded_bytes += len(part_payload)
			# Complete multipart upload after all chunks succeeded
			await asyncio.to_thread(
				self.s3.complete_multipart_upload,
				Bucket=self.bucket,
				Key=object_key,
				UploadId=upload_id,
				MultipartUpload={'Parts': parts},
			)
			# Compute elapsed seconds for final completion summary
			elapsed = max(time.monotonic() - start_time, 1e-6)
			# Compute final average MB/s for completion summary
			rate_mbps = (total_uploaded_bytes / (1024 * 1024)) / elapsed
			# Log final streamed upload completion summary
			mesologger.info(f"Stream upload complete {total_uploaded_bytes // (1024 * 1024)}MB to s3://{self.bucket}/{object_key} in {len(parts)} parts avg {rate_mbps:.2f}MB/s")
		except Exception:
			# Abort multipart upload so partial objects are not left behind
			await asyncio.to_thread(self.s3.abort_multipart_upload, Bucket=self.bucket, Key=object_key, UploadId=upload_id)
			# Re-raise original failure for caller handling
			raise


# Resolve runtime storage configuration from settings or environment
# and create matching backend instance

def create_storage():
	# Use global warning guard so we only log missing-config fallback once
	global _storage_warned_missing_config
	# Pull current settings instance when initialized
	settings_ready = getattr(settings, '_instance', None) is not None
	# Fall back to local storage while settings are uninitialized
	if not settings_ready: return LocalStorage()
	# Honor explicit runtime toggle and keep local mode by default
	if not getattr(settings, 'USE_S3', False): return LocalStorage()
	# Resolve S3 settings from canopy secrets-backed runtime config
	bucket = getattr(settings, 'S3_BUCKET', None)
	endpoint = getattr(settings, 'S3_ENDPOINT', None)
	region = getattr(settings, 'S3_REGION', None) or 'fsn1'
	access_key = getattr(settings, 'S3_ACCESS_KEY', None)
	secret_key = getattr(settings, 'S3_SECRET_KEY', None)
	# Fall back gracefully when S3 mode was requested but config is incomplete
	if not all([bucket, endpoint, access_key, secret_key]):
		# Emit one clear operator-facing warning for missing S3 config
		if not _storage_warned_missing_config:
			mesologger.info('Please provide necessary S3 config and secrets to use S3 mode')
			_storage_warned_missing_config = True
		# Keep pipeline functional by falling back to local storage
		return LocalStorage()
	# Create S3 backend for configured credentials
	return S3Storage(bucket, endpoint, access_key, secret_key, region)


# Return cached storage backend and rebuild when runtime config changes

def get_storage():
	# Use global cache references for backend reuse across imports
	global _storage_cache, _storage_signature
	# Pull current settings instance when initialized
	settings_ready = getattr(settings, '_instance', None) is not None
	# Build signature from runtime S3 toggle and secrets-backed config
	signature = (
		getattr(settings, 'USE_S3', False) if settings_ready else False,
		getattr(settings, 'S3_BUCKET', None) if settings_ready else None,
		getattr(settings, 'S3_ENDPOINT', None) if settings_ready else None,
		getattr(settings, 'S3_REGION', None) if settings_ready else None,
		getattr(settings, 'S3_ACCESS_KEY', None) if settings_ready else None,
		getattr(settings, 'S3_SECRET_KEY', None) if settings_ready else None,
	)
	# Rebuild backend when first requested or when config signature changed
	if _storage_cache is None or signature != _storage_signature:
		# Create backend matching current runtime config
		_storage_cache = create_storage()
		# Cache signature for next call
		_storage_signature = signature
	# Return active backend instance
	return _storage_cache


# Proxy object so modules can import `storage` and stay backend-agnostic
class StorageProxy:
	# Forward unknown attributes to active backend instance lazily
	def __getattr__(self, name):
		# Resolve backend and delegate attribute access
		return getattr(get_storage(), name)


# Shared lazy storage proxy used across canopy and wrapper modules
storage = StorageProxy()
