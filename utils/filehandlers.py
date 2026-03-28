# Load filesystem helpers, timing utilities, system access, and dynamic imports
import os, time, sys, importlib

# Load canopy path constants and runtime settings
from .. import PROCESSED_DIR, SRC_DIR, TMP_DIR, RELEASES_DIR, settings
# Load download helpers used by source fetch logic
from ..utils.downloader import pull, get_local_source_file

# Track wrapper-distilled release artifact prefixes for cleanup logic
releasefiles = ['precog','typesense','postgres','taxonext','citations','timeline','rarities']

# Fetch source metadata, optionally download updates, and decide whether processing is needed
async def fetch(session, source: dict) -> bool:
	# Fetch the latest downloaded file
	latest_download = get_local_source_file(source['name'])
	if latest_download: 
		source['latest_download'] = latest_download
		source['timestamp_download'] = int(latest_download.split('.')[1])
		print(f"IMPORT : Latest local version of { source['name'] } is from { source['timestamp_download'] }")
	else: print(f"IMPORT : No local { source['name'] } version available")
	# Check for remote version
	if settings.CHECK_FOR_DOWNLOADS: 
		# See if we have a new file successfully downloaded
		if await pull(session, source): print(f"IMPORT : Successfully fetched new remote version of { source['name'] }.")
	# If we haven't checked for the latest processed file yet	
	if not source.get('latest_processed'):
		# Fetch the latest processed file
		latest_processed = get_file(source['name'])
		if latest_processed: 
			source['latest_processed'] = latest_processed
			source['timestamp_processed'] = int(latest_processed.split('.')[1])
	# Check if we even have something to process at this point
	if not source.get('timestamp_download'): return False
	# Check if we need to process
	if not source.get('timestamp_processed') or (source.get('timestamp_download') and source.get('timestamp_processed') < source.get('timestamp_download')):
		print(f"IMPORT : Processed { source['name']} version outdated, we have { source.get('timestamp_processed') } but { source.get('timestamp_download') } is available.")
		# Let importer know that processing needs to be done
		return True
	return False

# Collect latest processed parquet per dataset for fuse fallback runs
def get_latest_processed() -> dict | None:
	sources = {} 
	datasets = []
	# Go through dir
	for file in os.listdir(PROCESSED_DIR):
		# Only consider parquet files
		if not file.endswith('.parquet'): continue
		# Get the dataset name
		name = file.split('.')[0]
		# Add to list
		if not name in datasets: datasets.append(name)
	# Go through dataset
	for dataset in datasets:
		item = get_file(dataset)
		# Append it to dict
		if item: sources[dataset] = { 
			'name': dataset, 
			'latest_processed': item, 
			'timestamp_processed': item.split('.')[1]
		}
		# Parametric import of 'source' dict
		module = importlib.import_module(f"importer.canopy.datasets.{dataset}")
		if module: 
			# Also handle Tropicos edge cases where we have multiple sources
			source_dict = (getattr(module, "source", None) or (getattr(module, "sources", [])[:1] or [None])[0])
			if source_dict and source_dict.get('citation'): sources[dataset]['citation'] = source_dict.get('citation')
	# Return
	return sources	


# Return newest parquet matching a dataset prefix in the target directory
def get_file(starts_with: str, dir=None) -> str | None:
	# Default to processed dir
	if not dir: dir = PROCESSED_DIR
	newest_file = None
	# Go through dir
	for file in os.listdir(dir):
		# See if we have a match
		if file.startswith(str(starts_with + '.')) and file.count('.') == 2 and os.path.splitext(file)[1] == '.parquet':
			# Compare timestamps
			if not newest_file or int(file.split('.')[1] > newest_file.split('.')[1]):
				# Assign the latest we found
				newest_file = file
	# Return None or our newest file
	return newest_file

# Delete older versioned files for one dataset after successful write/download
def delete_older_files(filename: str, datehash: str, dir) -> None:
	dir = dir or SRC_DIR
	# Prevent the most stupid mistakes
	if len(str(dir)) < 3:
		print(f"IMPORT : WARNING, TRIED TO DELETE FILES IN { dir }")
		return
	try:
		for file in os.listdir(dir):
			# Continue the loop if it's not a file (i.e. it's a directory)
			full_path = os.path.join(dir, file)
			if not os.path.isfile(full_path): continue
			# Ignore other files, make sure to use delimiting dot as we have wikispecies-foo etc
			if not file.startswith(filename + '.'): continue
			# Compare hashes, this shouldn't be larger but lets leave it in anyway for now
			if int(file.split('.')[1]) >= int(datehash): continue
			# Delete if we made it all the way here
			print(f"IMPORT : Deleting old file { dir }/{ file }")
			os.remove(f"{ dir }/{ file }")
	except Exception as e:
		print(f"IMPORT : Unable to delete { filename } {type(e).__name__ } { e }.")	

# Check whether a specific release folder exists in the target release directory
def check_release(release,dir=None):
	# Default to staging dir
	if not dir: dir = RELEASES_DIR
	release_path = os.path.join(dir, release)
	return os.path.exists(release_path) and os.path.isdir(release_path)

# Load latest release manifest based on YYYYMMDD-hash release folder naming
def get_latest_release(release_dir=None):
	import re, json
	newest_release = None
	newest_date = None
	# Default to staging dir
	if not release_dir: release_dir = RELEASES_DIR
	# Go through dir
	for entry in os.listdir(release_dir):	
		# Check if it's a proper release
		if os.path.isdir(os.path.join(release_dir, entry)) and re.match(r'^\d{8}-[a-f0-9]+$', entry):
			# Extract date part
			date_str = entry.split('-')[0]
			# If this is the first valid directory or newer than our current newest
			if newest_date is None or date_str > newest_date:
				newest_date = date_str
				newest_release = entry
	# If we still don't have a release
	if not newest_release:
		print(f"IMPORT : No release found in {release_dir}")		
		return	
	# Try fetching manifest
	try: 
		with open(os.path.join(release_dir, newest_release + '/manifest.json'), 'r') as f: return json.load(f)
	# Error logging
	except FileNotFoundError: print(f"IMPORT : No manifest found in { newest_release }")
	except json.JSONDecodeError: print(f"IMPORT : { newest_release } release manifest corrupted")	

# Remove stale hashed release artifacts that are no longer referenced by manifest
def cleanup_release(dir, manifest):
	# Full dir
	release_dir = os.path.join(dir,manifest.get('version'))
	# Prevent the most stupid mistakes
	if len(str(release_dir)) < 3:
		print(f"IMPORT : WARNING, TRIED TO DELETE FILES IN { release_dir }")
		return
	# Log
	print(f"IMPORT : Cleaning up release dir {release_dir}")
	try:
		# Do list comprehension only once
		current_files = [manifest[key] for key in releasefiles]
		for file in os.listdir(release_dir):
			# Continue the loop if it's not a file (i.e. it's a directory)
			full_path = os.path.join(release_dir, file)
			if not os.path.isfile(full_path): continue
			# Ignore other files, make sure to use delimiting dot as we have wikispecies-foo etc
			if not file.split('.')[0] in releasefiles: continue
			# Check if it's a file we actually want to keep
			if file in current_files: continue
			# Delete if we made it all the way here
			print(f"IMPORT : Deleting old file { release_dir }/{ file }")
			os.remove(f"{ release_dir }/{ file }")
	except Exception as e:
		print(f"IMPORT : Unable to delete file in { release_dir } {type(e).__name__ } { e }.")	

################## Parallel processing of very large gzip files like Wikidata from here on ###########################	

# For parallel file processing:
import multiprocessing, subprocess, psutil, uuid

# Proper logging
import signal
import atexit

# See what resources (cores and RAM) we have available for large file handling
def get_system_resources() -> list[int, int]:
    try:
        # Get CPU count - use cpu_count() for Windows compatibility
        cores = os.cpu_count()
        memory = int(psutil.virtual_memory().total / 1024**3)
        print(f"IMPORT : { cores} cores and { memory }GB memory available")
        return [cores, memory]
    except Exception as e:
        print(f"IMPORT : Unable to detect system cores and RAM, using 8GB and 8 threads {e}")
        return [8,8]

# Takes a gzipped file and filter criteria, splits the gzip in chunks, 
# runs them through ripgrep and produces one output file in temp dir
def filter_gzip(source: dict, pattern: str):
	print(f"IMPORT : Filtering large gzip file { source['latest_download']}")
	# Sanity
	file = os.path.join(SRC_DIR, source['latest_download'])
	if not os.path.isfile(file):
		print(f"IMPORT : File not found { source['latest_download']}")	
		return	
	# Get our chunks first
	chunks = get_gzip_chunks(file)
	# If we don't have any chunks, we can return (logging is in gzip header scanning logic)
	if not chunks: return
	# Spawn multiple filters decompressing, filtering and writing into our TMP_DIR
	result_files = process_chunks_parallel(file, chunks, pattern, get_system_resources()[0] // 3)
	try:
		filtered_filename = f"{ source['name'] }.{ source['timestamp_download']}.filtered"
		with open(os.path.join(TMP_DIR, filtered_filename), "w") as outfile:
			for chunk in result_files:
				# Extract just the filtered_filename part to check if it starts with "chunk_"
				if os.path.basename(chunk).startswith("chunk_"):
					print(f"IMPORT : Adding { chunk } to { filtered_filename }")
					try:
						# Use the full path when opening the file
						with open(chunk, "r") as infile: outfile.write(infile.read())
						# Delete the chunk file
						os.remove(chunk)
					except Exception as e: print(f"IMPORT : Error processing {chunk}: {e}")
		print(f"IMPORT : All { len(result_files) } chunks merged into { filtered_filename }")
		return filtered_filename
	except Exception as e:
		print(f"IMPORT : Error {e}")
	
# Split and scan gzipped file for gzip headers
def get_gzip_chunks(file):
	# Static
	header_magic = b'\x1f\x8b'
	buffer_size = 8192
	# Dynamic
	memory = get_system_resources()[1]
	file_size = os.path.getsize(file)
	file_size_gb = file_size / 1024**3
	num_chunks = int(file_size_gb / (memory / 12))
	chunk_size = file_size // num_chunks
	chunk_size_gb = chunk_size // 1024**3
	# Log
	print(f"IMPORT : Dividing { file } into { num_chunks } chunks of { chunk_size_gb }GB each")
	# First boundary is always 0
	boundaries = [0]
	start_time = time.time()
	
	def is_valid_gzip_header(f, pos):
		# Save current position
		current_pos = f.tell()
		try:
			# Go to the potential header position
			f.seek(pos)
			# Read first 10 bytes (minimum gzip header size)
			header = f.read(10)
			
			# Return False if we don't have enough bytes
			if len(header) < 10:
				return False
			
			# Check magic bytes
			if header[0:2] != header_magic:
				return False
			
			# Check compression method (should be 8 for DEFLATE)
			if header[2] != 8:
				return False
			
			# Try to actually read some decompressed data to validate
			f.seek(pos)
			try:
				import gzip
				decompressor = gzip.GzipFile(fileobj=f, mode='rb')
				# Try to read a small amount of decompressed data
				test_data = decompressor.read(1024)
				return len(test_data) > 0
			except Exception:
				return False
		finally:
			# Restore position
			f.seek(current_pos)
	
	# Open file
	with open(file, 'rb') as f:
		# go through each chunk
		for i in range(1, num_chunks):
			eof = False
			# Get position
			header_pos = i * chunk_size
			# Go to position
			f.seek(header_pos)
			# Keep looping
			while True:
				# Read into buffer
				buffer = f.read(buffer_size)
				# If we reached end of file
				if not buffer:
					eof = True
					break
				# Check for magic gzip header byte
				idx = buffer.find(header_magic)
				# If we found it
				if idx != -1:
					potential_header_pos = header_pos + idx
					# Validate if it's actually a gzip header
					if is_valid_gzip_header(f, potential_header_pos):
						# Add the position (offset plus index position)
						boundaries.append(potential_header_pos)
						print(f"IMPORT : Found valid header at position {potential_header_pos/1024/1024/1024:.2f} GB")
						break
					else:
						# False positive, continue searching after this position
						header_pos += idx + 2
						f.seek(header_pos)
						continue
				# Move position forward, but back up 1 byte in case the header spans chunks
				header_pos += len(buffer) - 1
				f.seek(header_pos)
			# In case we haven't found anything
			if eof:
				print(f"IMPORT : No header found in chunk { i }, after position {header_pos/1024/1024/1024:.2f} GB")
				return False
	# Add the file size as the last boundary
	boundaries.append(file_size)
	end_time = time.time()
	print(f"IMPORT : Found all {len(boundaries)-1} chunk headers in {end_time - start_time:.2f} seconds")
	return boundaries

# Global flag for tracking termination
_terminate = False

# List to keep track of all child processes
_all_processes = []

def cleanup_processes():
	"""Kill all registered processes on exit"""
	global _all_processes
	for proc in _all_processes:
		try:
			if hasattr(proc, 'terminate'):
				proc.terminate()
			elif hasattr(proc, 'kill'):
				proc.kill()
		except:
			pass

# Register cleanup on exit
atexit.register(cleanup_processes)

def process_chunks_parallel(archive_path, chunks, pattern, max_workers=2):
	"""
	Process multiple chunks of the gzipped archive in parallel.
   
	Args:
		archive_path: Path to the gzipped archive
		chunks: List of chunk offsets
		pattern: Pattern to search for using ripgrep
		max_workers: Maximum number of parallel workers (defaults to CPU count)
       
	Returns:
		List of paths to temporary files containing matches
	"""
	global _terminate
	_terminate = False
	
	print(f"IMPORT : Starting parallel processing with {max_workers} workers")
	
	# Set up signal handler for Ctrl+C
	def sigint_handler(sig, frame):
		global _terminate
		print("IMPORT : Received Ctrl+C. Aborting all processes...")
		_terminate = True
		
		# Call cleanup immediately
		cleanup_processes()
		
		# Force exit on second Ctrl+C
		signal.signal(signal.SIGINT, lambda s, f: os._exit(1))
	
	original_sigint_handler = signal.getsignal(signal.SIGINT)
	signal.signal(signal.SIGINT, sigint_handler)
	
	# Prepare chunk arguments
	chunk_args = []
	for i in range(len(chunks) - 1):
		chunk_args.append((archive_path, chunks[i], pattern, chunks[i+1], i))
	
	# Process chunks in parallel using direct multiprocessing
	results = []
	processes = []
	result_queue = multiprocessing.Queue()
	
	try:
		# Start at most max_workers processes
		running_processes = 0
		next_chunk = 0
		
		# Continue loop until all chunks are assigned AND all processes are complete
		while (next_chunk < len(chunk_args) or processes) and not _terminate:
			# Start processes up to max_workers
			while running_processes < max_workers and next_chunk < len(chunk_args):
				if _terminate:
					break
					
				args = chunk_args[next_chunk]
				p = multiprocessing.Process(
					target=process_chunk_wrapper,
					args=(args, result_queue)
				)
				p.daemon = True  # Set as daemon so it exits when main process exits
				p.start()
				processes.append(p)
				_all_processes.append(p)  # Add to global list for cleanup
				running_processes += 1
				next_chunk += 1
			
			# Check for completed processes and results
			for p in list(processes):
				if not p.is_alive():
					processes.remove(p)
					running_processes -= 1
			
			# Check for results without blocking
			while not result_queue.empty():
				result = result_queue.get_nowait()
				if result is not None:
					results.append(result)
			
			# Small sleep to prevent CPU hogging
			time.sleep(0.1)
		
		# Wait for remaining processes to finish or terminate them
		if _terminate:
			for p in processes:
				if p.is_alive():
					p.terminate()
		else:
			# Wait for all processes to complete
			for p in processes:
				p.join(timeout=1)
				if p.is_alive():
					p.terminate()
			
			# Get any remaining results
			while not result_queue.empty():
				result = result_queue.get_nowait()
				if result is not None:
					results.append(result)
	
	except KeyboardInterrupt:
		print("IMPORT : Interrupt received in main process. Terminating all workers...")
		_terminate = True
		
		# Terminate all processes
		for p in processes:
			if p.is_alive():
				p.terminate()
	
	finally:
		# Clean up
		signal.signal(signal.SIGINT, original_sigint_handler)
		
		if _terminate:
			print(f"IMPORT : Processing aborted. Processed {len(results)}/{len(chunk_args)} chunks before abort")
		else:
			print(f"IMPORT : Parallel processing complete. Processed {len(results)}/{len(chunk_args)} chunks successfully")
	
	return results

def process_chunk_wrapper(args, result_queue):
	"""Wrapper to handle process_chunk and put result in queue"""
	try:
		# Set up process-specific signal handler
		def proc_sigint_handler(sig, frame):
			# Just exit the process
			sys.exit(0)
		
		signal.signal(signal.SIGINT, proc_sigint_handler)
		
		# Process the chunk
		result = process_chunk(*args)
		
		# Put result in queue
		result_queue.put(result)
	except KeyboardInterrupt:
		# Handle interrupt gracefully
		sys.exit(0)
	except Exception as e:
		print(f"IMPORT : Error in worker process: {str(e)}")
		result_queue.put(None)
		sys.exit(1)

def process_chunk(archive_path, chunk_offset, pattern, next_chunk_offset, chunk_id):
	"""
	Process a single chunk of the gzipped archive starting at chunk_offset.
	Uses pigz to decompress and ripgrep to filter, writing results to a temporary file.
   
	Args:
		archive_path: Path to the gzipped archive
		chunk_offset: Byte offset of the gzip header to start from
		pattern: Pattern to search for using ripgrep
		next_chunk_offset: Byte offset of the next chunk (to limit reading)
		chunk_id: Identifier for this chunk (used for progress tracking)
	   
	Returns:
		Path to the temporary file containing matches or None if an error occurred
	"""
	global _terminate, _all_processes
	
	# Generate a random filename for the temporary results
	temp_file = os.path.join(TMP_DIR, f"chunk_{uuid.uuid4().hex}.txt")
   
	# Calculate chunk size
	chunk_size = next_chunk_offset - chunk_offset
   
	# Buffer size optimized for 5GB chunks and multiple parallel processes
	# Using 16MB as a good balance for large chunks
	buffer_size = 64 * 1024 * 1024  # 16MB buffer
   
	# Prepare ripgrep command to filter the results with performance optimizations
	# Using --binary for explicit binary mode and mmap for faster file access
	rg_cmd = ["rg", pattern, "--no-line-number", "--no-filename", "--binary", "--mmap", "--dfa-size-limit=100M"]
   
	pigz_process = None
	rg_process = None
	
	try:
		# Start pigz process for decompression with focus on I/O optimization
		# -b 512: Use 512k block size for better throughput
		pigz_process = subprocess.Popen(["pigz", "-d", "-c", "-b", "512"], stdin=subprocess.PIPE, stdout=subprocess.PIPE, stderr=subprocess.PIPE, bufsize=buffer_size)
		_all_processes.append(pigz_process)
	   
		# Start ripgrep process for filtering with binary mode
		with open(temp_file, "wb") as output_file:
			rg_process = subprocess.Popen(rg_cmd, stdin=pigz_process.stdout, stdout=output_file, stderr=subprocess.PIPE, bufsize=buffer_size)
			_all_processes.append(rg_process)
		   
			# Close pigz's stdout in the parent process
			pigz_process.stdout.close()
		
		# Open the archive file and feed the chunk to pigz using memory mapping when available
		with open(archive_path, "rb") as archive_file:
			# Seek to the chunk offset
			archive_file.seek(chunk_offset)
			print(f"IMPORT : Starting to process chunk { chunk_id }")			
			# Read and feed data in chunks to avoid excessive memory usage
			bytes_remaining = chunk_size
			start_time = time.time()
			total_processed = 0
			last_log_time = time.time()
			
			while bytes_remaining > 0:
				# Check for termination request
				if _terminate:
					raise KeyboardInterrupt("Processing terminated")
					
				# Calculate how much to read in this iteration
				read_size = min(buffer_size, bytes_remaining)
			   
				# Read a chunk from the file
				data = archive_file.read(read_size)
				if not data:  # EOF reached
					break
				
				# Track throughput
				chunk_size_bytes = len(data)
				total_processed += chunk_size_bytes
				
				# Write to pigz's stdin
				pigz_process.stdin.write(data)
			   
				# Update remaining bytes
				bytes_remaining -= chunk_size_bytes
				
				# Only log every second to avoid overwhelming with messages
				current_time = time.time()
				if current_time - last_log_time >= 1.0:
					elapsed = current_time - start_time
					mb_per_sec = (total_processed / 1024 / 1024) / elapsed if elapsed > 0 else 0					
					print(f"IMPORT : Chunk {chunk_id}: Remaining: {bytes_remaining/1024/1024:.2f} MB | Processed: {total_processed/1024/1024:.2f} MB | Speed: {mb_per_sec:.2f} MB/s")
					
					last_log_time = current_time
			
			# Final stats after processing all data
			elapsed = time.time() - start_time
			mb_per_sec = (total_processed / 1024 / 1024) / elapsed if elapsed > 0 else 0
			
			print(f"IMPORT : CHUNK {chunk_id} COMPLETE Total processed: {total_processed/1024/1024:.2f} MB | Avg Speed: {mb_per_sec:.2f} MB/s")
			
			# Close pigz's stdin to signal end of input
			pigz_process.stdin.close()
	   
		# Only wait for processes if not terminating
		if not _terminate:
			# Wait for processes to complete with a reasonable timeout
			pigz_exit_code = pigz_process.wait(timeout=600)  # 10 min timeout
			rg_exit_code = rg_process.wait(timeout=600)
		   
			# Check for errors - handle gzip corruption more gracefully
			if pigz_exit_code != 0:
				pigz_error = pigz_process.stderr.read().decode('utf-8', errors='replace')
				if "corrupted input" in pigz_error and bytes_remaining == 0:
					# This might be expected if we're cutting across gzip stream boundaries
					if settings.VERBOSE: print(f"IMPORT : Possible gzip boundary at end of chunk, processing as much as possible")
					return temp_file
				else:
					print(f"IMPORT : pigz error (code {pigz_exit_code}): {pigz_error}")
					return None
			   
			if rg_exit_code != 0 and rg_exit_code != 1:  # ripgrep returns 1 when no matches found
				rg_error = rg_process.stderr.read().decode('utf-8', errors='replace')
				print(f"IMPORT : ripgrep error (code {rg_exit_code}): {rg_error}")
				return None
		   
			return temp_file
		else:
			return None
	   
	except subprocess.TimeoutExpired:
		print(f"IMPORT : Timeout processing chunk at offset {chunk_offset}")
		return None
		
	except KeyboardInterrupt:
		print(f"IMPORT : Chunk {chunk_id} aborted by user")
		return None
		
	except Exception as e:
		print(f"IMPORT : Error processing chunk at offset {chunk_offset}: {str(e)}")
		return None
	
	finally:
		# Kill processes in this process
		if pigz_process and hasattr(pigz_process, 'poll') and pigz_process.poll() is None:
			try: pigz_process.kill()
			except: pass
		if rg_process and hasattr(rg_process, 'poll') and rg_process.poll() is None:
			try: rg_process.kill()
			except: pass