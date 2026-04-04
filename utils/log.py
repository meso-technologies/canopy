# Load logging primitives for structured importer logs
import logging
# Load environment helpers for optional sentry setup
import os
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
			print(line, end='\r', flush=True)
			# Mark active same-line mode for next non-sameline message
			self.sameline_active = True
		# Otherwise print regular newline-terminated log line
		else:
			# Close previous same-line progress row before normal output
			if self.sameline_active: print()
			# Print normal line with newline terminator
			print(line)
			# Reset same-line tracking after normal line
			self.sameline_active = False
		# Pull structured payload if provided by caller
		payload = getattr(record, 'payload', None)
		# Print structured payload as pretty json when available
		if payload is not None:
			# Ensure payload starts on a clean line after same-line progress
			if self.sameline_active:
				# Finish same-line mode before payload output
				print()
				# Reset same-line tracking after explicit line break
				self.sameline_active = False
			# Print payload as pretty JSON
			print(json.dumps(payload, indent=2, default=str))

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
	# Read optional sentry dsn from environment
	dsn = os.environ.get('CANOPY_SENTRY_DSN', '')
	# Initialize sentry once when configured and dependency is available
	if dsn and sentry_sdk and not sentry_sdk.is_initialized():
		# Initialize sentry with importer-specific defaults
		sentry_sdk.init(
			dsn=dsn,
			environment=os.environ.get('CANOPY_SENTRY_ENV', 'development'),
			release=f"canopy@{os.environ.get('CANOPY_VERSION', 'unknown')}",
			send_default_pii=False,
			traces_sample_rate=0.0,
			enable_logs=True,
		)
	# Return configured logger for immediate use
	return logger

# Expose module-level canopy logger reference
mesologger = logging.getLogger('canopy')
