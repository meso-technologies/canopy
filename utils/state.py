# Load filesystem path helpers for cross-platform state manifest location
import os
# Load JSON time helpers for run-state metadata
from datetime import datetime, timezone

# Load canopy data root for stable state manifest location
from .. import DATA_DIR
# Load shared storage proxy for local/S3 transparent JSON access
from ..utils.s3 import storage

# Resolve canonical state manifest path used across all canopy stages
STATE_MANIFEST_PATH = os.path.join(DATA_DIR, 'state', 'manifest.json')

# Load current state manifest or return an empty baseline
def load_state() -> dict:
	# Try reading existing state manifest from active storage backend
	try:
		# Read manifest payload from active storage backend
		state = storage.read_json(STATE_MANIFEST_PATH)
		# Ensure sources map exists for downstream source updates
		if 'sources' not in state or not isinstance(state.get('sources'), dict): state['sources'] = {}
		# Ensure checkpoints map exists for stage-level timestamps
		if 'checkpoints' not in state or not isinstance(state.get('checkpoints'), dict): state['checkpoints'] = {}
		# Return normalized state payload
		return state
	# Fall back to empty manifest when state file does not exist yet
	except FileNotFoundError: return {'sources': {}, 'checkpoints': {}, 'updated_at': None}

# Persist full state manifest to active storage backend
def save_state(state: dict):
	# Stamp top-level update timestamp for audit/debug visibility
	state['updated_at'] = datetime.now(timezone.utc).isoformat()
	# Write state manifest JSON through storage abstraction
	storage.write_json(STATE_MANIFEST_PATH, state)

# Update one source entry in state manifest with latest known metadata
def update_source_state(source: dict, stage: str | None = None):
	# Abort when source payload is missing or unnamed
	if not source or not source.get('name'): return
	# Load existing state manifest before mutating one source section
	state = load_state()
	# Ensure sources map exists in case of legacy/empty manifests
	if 'sources' not in state or not isinstance(state.get('sources'), dict): state['sources'] = {}
	# Get existing source state entry for partial-field updates
	existing = state['sources'].get(source['name'], {})
	# Build merged source payload starting from previous known values
	merged = dict(existing)
	# Keep lightweight stable fields only to avoid runtime-only noise
	for key in ['url', 'citation', 'latest_download', 'timestamp_download', 'timestamp_remote', 'timestamp_local', 'latest_processed', 'timestamp_processed']:
		# Overlay field when caller provided a concrete value
		if source.get(key) is not None: merged[key] = source.get(key)
	# Stamp per-source update time for incremental debugging
	merged['updated_at'] = datetime.now(timezone.utc).isoformat()
	# Store stage hint when provided by caller
	if stage: merged['last_stage'] = stage
	# Save merged source payload back into state manifest
	state['sources'][source['name']] = merged
	# Persist updated manifest
	save_state(state)

# Return state metadata for one source name if present
def get_source_state(name: str) -> dict | None:
	# Abort for empty source names
	if not name: return None
	# Load current state manifest
	state = load_state()
	# Return stored source payload or None when absent
	return state.get('sources', {}).get(name)

# Stamp a stage checkpoint with the current UTC timestamp
def set_checkpoint(name: str):
	# Abort when checkpoint name is empty
	if not name: return
	# Load existing state manifest before mutating checkpoints section
	state = load_state()
	# Ensure checkpoints map exists before writing one entry
	if 'checkpoints' not in state or not isinstance(state.get('checkpoints'), dict): state['checkpoints'] = {}
	# Create or replace checkpoint payload with minimal timestamp-only schema
	state['checkpoints'][name] = {'timestamp': datetime.now(timezone.utc).isoformat()}
	# Persist updated state manifest
	save_state(state)
