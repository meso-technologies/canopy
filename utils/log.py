# Load logging primitives for structured importer logs
import logging
# Load environment helpers for optional sentry setup
import os
# Load stdout/stderr helpers for broken-pipe safe console logging
import sys
# Load subprocess helpers for git release discovery
import subprocess
# Load json encoding for optional structured payload output
import json
# Load UTC timestamp helpers for consistent log prefixes
from datetime import datetime, timezone

# Load optional sentry sdk when installed
try:
	# Import sentry sdk for optional initialization
	import sentry_sdk
# Gracefully continue when sentry sdk is not installed
except ImportError:
	# Keep sentry sdk disabled in environments without dependency
	sentry_sdk = None

# Render canopy/importer log lines with platform tag and colorized levels
class CanopyHandler(logging.Handler):
	# Initialize handler with a platform tag shown in each log line
	def __init__(self, platform='CANOPY'):
		# Initialize base logging handler state
		super().__init__()
		# Store platform tag for runtime display switching
		self.platform = platform
		# Track whether previous log used carriage-return same-line mode
		self.sameline_active = False
		# Stop console writes after stdout closes during operator-terminated runs
		self.console_broken = False

	# Write one console fragment while tolerating closed stdout pipes
	def _safe_print(self, *args, **kwargs):
		# Skip console output once stdout is known closed
		if self.console_broken: return
		# Try normal console output first
		try: print(*args, **kwargs)
		# Suppress broken pipe noise from killed systemd/tee/journal streams
		except BrokenPipeError:
			# Mark console as unavailable for remaining log records
			self.console_broken = True
			# Redirect stdout to devnull so any external print fallback does not recurse noisily
			try: sys.stdout = open(os.devnull, 'w')
			# Ignore devnull redirect failures because logging must never crash the process
			except Exception: pass

	# Emit one fully formatted console log line
	def emit(self, record):
		# Build UTC timestamp matching existing service logger format
		utc = datetime.now(timezone.utc).strftime('%H:%M:%S,%f')[:-3]
		# Read relative milliseconds from logging record
		ms = record.relativeCreated
		# Format relative runtime as ms for short and s for long runs
		ts = f"{ms/1000:.2f}s" if ms > 1000 else f"{int(ms)}ms"
		# Resolve ANSI color code for the current log level
		color = self._color(record.levelno)
		# Build one prefixed and colorized log line
		line = f"\033[1m{self.platform}\033[0m {color}{utc} [{ts}]\033[0m {record.getMessage()}"
		# Read optional same-line rendering flag from log record
		sameline = bool(getattr(record, 'sameline', False))
		# Print same-line progress updates with carriage return
		if sameline:
			# Render progress line without newline for in-place updates
			self._safe_print(line, end='\r', flush=True)
			# Mark active same-line mode for next non-sameline message
			self.sameline_active = True
		# Otherwise print regular newline-terminated log line
		else:
			# Close previous same-line progress row before normal output
			if self.sameline_active: self._safe_print()
			# Print normal line with newline terminator
			self._safe_print(line)
			# Reset same-line tracking after normal line
			self.sameline_active = False
		# Pull structured payload if provided by caller
		payload = getattr(record, 'payload', None)
		# Print structured payload as pretty json when available
		if payload is not None:
			# Ensure payload starts on a clean line after same-line progress
			if self.sameline_active:
				# Finish same-line mode before payload output
				self._safe_print()
				# Reset same-line tracking after explicit line break
				self.sameline_active = False
			# Print payload as pretty JSON
			self._safe_print(json.dumps(payload, indent=2, default=str))

	# Map python logging levels to ANSI console colors
	def _color(self, levelno):
		# Use bright red for critical/fatal events
		if levelno >= logging.CRITICAL: return '\033[91m'
		# Use red for errors
		if levelno >= logging.ERROR: return '\033[31m'
		# Use yellow for warnings
		if levelno >= logging.WARNING: return '\033[33m'
		# Use green for info
		if levelno >= logging.INFO: return '\033[32m'
		# Use blue for debug/trace style output
		if levelno >= logging.DEBUG: return '\033[94m'
		# Use gray for anything below debug
		return '\033[90m'

# Resolve Sentry DSN from env first and Meso production secrets second
def _resolve_sentry_dsn():
	# Prefer explicit env var so open-source and local users can opt in without Meso secrets
	dsn = os.environ.get('CANOPY_SENTRY_DSN', '')
	# Return explicit DSN when present
	if dsn: return dsn
	# Try Meso production secrets without requiring them for standalone canopy users
	try:
		# Load optional deployed canopy secrets module
		from importer.canopy.config import secrets
		# Return production importer DSN when Ansible rendered one
		return getattr(secrets, 'SENTRY_DSN_IMPORTER', '') or ''
	# Ignore missing or incomplete secrets modules in open-source and local installs
	except Exception: return ''

# Resolve Sentry environment while keeping standalone env-var opt-in as development by default
def _resolve_sentry_environment(dsn):
	# Prefer explicit environment override for local tests and future staging
	environment = os.environ.get('CANOPY_SENTRY_ENV', '')
	# Return explicit environment when provided
	if environment: return environment
	# Treat bundled Meso production secret usage as production unless overridden
	try:
		# Load optional deployed canopy secrets module
		from importer.canopy.config import secrets
		# Compare DSN against rendered production secret value
		if dsn and dsn == getattr(secrets, 'SENTRY_DSN_IMPORTER', ''): return 'production'
	# Ignore missing secrets in standalone canopy installs
	except Exception: pass
	# Keep env-var-only standalone usage out of production by default
	return 'development'

# Resolve release identifier from env var or git hash when available
def _resolve_sentry_release():
	# Prefer deployment-provided version when present
	version = os.environ.get('CANOPY_VERSION', '')
	# Return explicit release string when provided by caller
	if version: return f"canopy@{version}"
	# Try to discover current git hash from the deployed checkout
	try:
		# Ask git for short commit id without emitting errors in tarball installs
		result = subprocess.run(['git', 'rev-parse', '--short', 'HEAD'], capture_output=True, text=True, timeout=2)
		# Use git hash when command succeeded
		if result.returncode == 0 and result.stdout.strip(): return f"canopy@{result.stdout.strip()}"
	# Ignore git lookup failures in cloud bundles and source archives
	except Exception: pass
	# Fall back on stable unknown release token
	return 'canopy@unknown'

# Flush Sentry envelopes before short-lived importer/cruncher processes exit
def mesologger_flush(timeout=2):
	# Skip flush when sentry sdk is unavailable or inactive
	if not sentry_sdk or not sentry_sdk.is_initialized(): return
	# Flush queued Sentry events and logs before process exit
	sentry_sdk.flush(timeout=timeout)

# Configure canopy logger once and optionally initialize sentry
def mesologger_setup(platform='CANOPY'):
	# Get shared canopy logger instance
	logger = logging.getLogger('canopy')
	# Keep full level range enabled for handler filtering
	logger.setLevel(logging.DEBUG)
	# Prevent duplicate propagation to root handlers
	logger.propagate = False
	# Attach canopy handler once and update platform tag on reuse
	if not logger.handlers:
		# Attach first handler with requested platform tag
		logger.addHandler(CanopyHandler(platform=platform))
	# Otherwise update existing canopy handler platform tag
	else:
		# Iterate existing handlers to find canopy handler instance
		for handler in logger.handlers:
			# Update handler platform tag when compatible handler exists
			if isinstance(handler, CanopyHandler): handler.platform = platform
	# Resolve optional sentry dsn from env or deployed Meso secrets
	dsn = _resolve_sentry_dsn()
	# Initialize sentry once when configured and dependency is available
	if dsn and sentry_sdk and not sentry_sdk.is_initialized():
		# Initialize sentry with importer-specific defaults
		sentry_sdk.init(
			dsn=dsn,
			environment=_resolve_sentry_environment(dsn),
			release=_resolve_sentry_release(),
			send_default_pii=False,
			traces_sample_rate=0.0,
			enable_logs=True,
		)
		# Tag importer logs by runtime surface for Sentry filtering
		sentry_sdk.set_tag('component', platform.lower())
		# Add region when provided by service environment
		if os.environ.get('CANOPY_REGION'): sentry_sdk.set_tag('region', os.environ.get('CANOPY_REGION'))
	# Return configured logger for immediate use
	return logger

# Expose module-level canopy logger reference
mesologger = logging.getLogger('canopy')
