# Load traceback helpers for readable failure output
import traceback
# Load process exit helper for non-zero failure signaling
import sys
# Load CLI argument parsing
import argparse
# Load async runtime primitives
import asyncio
# Load async HTTP session client for download handlers
import aiohttp
# Load canopy settings proxy and settings builder
from . import settings, build_settings

# Define source execution order so fusion gets expected dependencies
sources = ['ipni', 'fungorum', 'wcvp', 'powo', 'wfo', 'col', 'tropicos', 'mycobank', 'bhl', 'gbif', 'wikidata', 'wikispecies', 'inaturalist', 'iucn', 'ncbi']

# Run canopy stages based on CLI flags and return release metadata when available
async def main(argv=None):
	# Initialize CLI parser for standalone canopy usage
	parser = argparse.ArgumentParser()
	# Allow scoping to a single dataset for faster iteration/debugging
	parser.add_argument('-d', '--dataset', help='Specify dataset to import')
	# Enable verbose intermediate table output for diagnostics
	parser.add_argument('-v', '--verbose', action='store_true', help='Show analytics and table status in intermediary steps')
	# Force recomputation even when timestamp checks would skip work
	parser.add_argument('-f', '--force', action='store_true', help='Force import even if data exists')
	# Write TSV sidecars in addition to parquet outputs
	parser.add_argument('--csv', action='store_true', help='Write processed files as csv, in addition to parquet')
	# Activate debug profile with reduced workload knobs
	parser.add_argument('--debug', action='store_true', help='Fast partial processing for local dev loops')
	# Download new source files without processing/fusion
	parser.add_argument('--download', action='store_true', help='Download source datasets only')
	# Execute dataset processing stage
	parser.add_argument('--process', action='store_true', help='Process source datasets')
	# Execute fusion stage from processed source outputs
	parser.add_argument('--fuse', action='store_true', help='Fuse latest processed data')
	# Execute geospatial enrichment stage
	parser.add_argument('--geo', action='store_true', help='Geospatial processing')
	# Execute new polars-based geospatial pipeline (testing)
	parser.add_argument('--geo-new', action='store_true', help='New polars-based geospatial processing')
	# Execute API-backed enrichment stage (Wikipedia abstracts)
	parser.add_argument('--apis', action='store_true', help='Update API-backed enrichment datasets like Wikipedia abstracts')
	# Parse CLI args (or injected argv from wrapper)
	args = parser.parse_args(argv)
	# Build runtime settings class from CLI profile/flags
	runtime = build_settings(args)
	# Mark pure download mode so dataset handlers skip processing work
	runtime.DOWNLOAD_ONLY = bool(args.download and not args.process)
	# Publish runtime settings globally for canopy modules
	settings.set_config(runtime)
	# Announce canopy start for long-running logs
	print('CANOPY : Starting canopy pipeline')
	# Set a shared request timeout for direct HTTP operations
	timeout = aiohttp.ClientTimeout(total=60 * 120)
	# Protect full pipeline execution with top-level error handling
	try:
		# Track stage-level failures so wrapper can stop on canopy errors
		had_errors = False
		# Collect per-source processing metadata for downstream fusion/diff
		results = {}
		# Run source stage when explicitly requested or when no later-only flags were provided
		run_processing = args.process or args.download or not any([args.fuse, args.geo, getattr(args, 'geo_new', False), args.apis])
		# Start dataset execution stage when requested
		if run_processing:
			# Reuse one HTTP session across all dataset handlers
			async with aiohttp.ClientSession(timeout=timeout) as session:
				# Handle grouped async exceptions while keeping per-dataset tracebacks
				try:
					# Iterate in stable source order expected by later stages
					for source in sources:
						# Skip non-selected sources when dataset filter is active
						if args.dataset and args.dataset != source: continue
						# Build module path dynamically for current dataset
						module_path = f'{__package__}.datasets.{source}'
						# Build expected async update function name
						func_name = f'update_{source}'
						# Import dataset module lazily to avoid paying all import costs upfront
						module = __import__(module_path, fromlist=[func_name])
						# Resolve dataset update coroutine
						update_func = getattr(module, func_name)
						# Execute dataset update coroutine
						result = await update_func(session)
						# Keep source manifest data for fuse/diff stage input
						if result is not None: results[result['name']] = result
					# Guard against typos when dataset filter references unknown source names
					if args.dataset and args.dataset not in sources:
						# Report invalid dataset selection to the caller
						print('CANOPY : Invalid dataset specified')
						# Fail fast so wrapper can stop and report invalid CLI input
						raise ValueError('Invalid dataset specified')
				# Handle cooperative cancellation cleanly
				except* asyncio.CancelledError:
					# Mark grouped cancellation as failure for wrapper orchestration
					had_errors = True
					# Log cancellation reason for operator visibility
					print('CANOPY : Tasks cancelled - shutting down...')
				# Report grouped stage exceptions without losing stack traces
				except* Exception as eg:
					# Mark grouped dataset failures so process exits non-zero
					had_errors = True
					# Announce grouped dataset failures
					print('CANOPY : Some tasks failed:')
					# Print each underlying exception from the exception group
					for e in eg.exceptions:
						# Emit short exception summary
						print(f' - {type(e).__name__}: {str(e)}')
						# Emit full traceback for root-cause debugging
						traceback.print_exception(type(e), e, e.__traceback__)
		# Exit after downloads when explicitly running download-only mode
		if runtime.DOWNLOAD_ONLY:
			# Explain why follow-up stages are skipped
			print('CANOPY : Download-only run complete, skipping process/fuse/geo/apis steps')
			# No release object is produced in download-only runs
			return None
		# Initialize release metadata container for downstream stages
		release = None
		# Run fusion stage only when explicitly requested
		if args.fuse:
			# Isolate fusion errors so other diagnostics still print cleanly
			try:
				# Import fusion entrypoint lazily to avoid heavy import cost on download-only runs
				from .pipeline.fuse import fuse
				# Execute fusion with currently collected source metadata
				release = fuse(results)
			# Handle user aborts during long fusion queries
			except KeyboardInterrupt:
				# Mark cancellation as failure for wrapper orchestration
				had_errors = True
				# Confirm clean cancellation to operator logs
				print('CANOPY : Fusion cancelled by user.')
			# Surface fusion failures with full traceback context
			except Exception as e:
				# Mark fusion failure so process exits non-zero
				had_errors = True
				# Emit concise error summary for quick scanning
				print(f'CANOPY : Error during fusion: {type(e).__name__}: {str(e)}')
				# Emit full traceback for debugging
				traceback.print_exception(type(e), e, e.__traceback__)
		# Abort follow-up stages immediately when earlier stages failed
		if had_errors: raise RuntimeError('Canopy stage errors detected')
		# Run geospatial stage only when requested
		if args.geo:
			try:
				# Import geo stage lazily to avoid optional dependency cost unless needed
				from .pipeline.geo import compute_geospatial
				# Execute geospatial enrichment against the chosen release
				await compute_geospatial(release)
			except Exception as e:
				# Mark geo failure so process exits non-zero
				had_errors = True
				# Emit concise error summary for quick scanning
				print(f'CANOPY : Error during geo: {type(e).__name__}: {str(e)}')
				# Emit full traceback for debugging
				traceback.print_exception(type(e), e, e.__traceback__)
		# Run new polars-based geo pipeline when requested
		if getattr(args, 'geo_new', False):
			try:
				# Import new geo pipeline
				from .pipeline.geo_new import load_occurrences, rollup_to_parents, build_habitat_maps
				# Fall back to latest release if none provided
				if not release:
					from .utils.filehandlers import get_latest_release
					release = get_latest_release()
				# Run the polars pipeline
				compact = load_occurrences(release)
				compact = rollup_to_parents(compact, release)
				habitat = build_habitat_maps(compact)
				# Log result summary
				print(f"IMPORT : New geo pipeline complete: {len(habitat):,} habitat tiles across {habitat['gbif_id'].n_unique():,} taxa")
			except Exception as e:
				had_errors = True
				print(f'CANOPY : Error during geo-new: {type(e).__name__}: {str(e)}')
				traceback.print_exception(type(e), e, e.__traceback__)
		# Run API enrichment stage only when requested
		if args.apis:
			try:
				# Import Wikipedia updater lazily for optional API dependency paths
				from .datasets.wikipedia import update_wikipedia
				# Execute API refresh/update workflow
				update_wikipedia(release)
			except Exception as e:
				# Mark API stage failure so process exits non-zero
				had_errors = True
				# Emit concise error summary for quick scanning
				print(f'CANOPY : Error during apis: {type(e).__name__}: {str(e)}')
				# Emit full traceback for debugging
				traceback.print_exception(type(e), e, e.__traceback__)
		# Abort with non-zero exit semantics when any stage failed
		if had_errors: raise RuntimeError('Canopy stage errors detected')
		# Return release metadata for wrapper orchestration and distill handoff
		return release
	# Catch any unhandled runtime failure in canopy flow
	except Exception as e:
		# Emit concise top-level failure summary
		print(f'CANOPY : Unhandled exception {type(e).__name__}: {str(e)}')
		# Emit full traceback for postmortem debugging
		traceback.print_exception(type(e), e, e.__traceback__)
		# Re-raise so CLI process exits non-zero and wrapper can stop
		raise
	# Catch cancellation propagated to top-level runner
	except asyncio.exceptions.CancelledError:
		# Confirm cancellation in logs
		print('CANOPY : Pipeline cancelled.')

# Run canopy when invoked as a module script
if __name__ == '__main__':
	# Wrap standalone execution with explicit shutdown/error logging
	try:
		# Start async canopy main coroutine
		asyncio.run(main())
	# Handle Ctrl+C shutdown cleanly
	except KeyboardInterrupt:
		# Confirm graceful stop in logs
		print('CANOPY : Shutting down canopy...')
	# Catch any unhandled bootstrap/runtime error in standalone mode
	except Exception as e:
		# Emit concise top-level error message
		print(f'CANOPY : Unhandled error {type(e).__name__}: {e}.')
		# Emit stack for debugging unexpected top-level failures
		traceback.print_stack()
		# Exit non-zero so wrapper orchestration can stop safely
		sys.exit(1)
