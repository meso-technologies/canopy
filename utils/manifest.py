# Resolve diff sidecar filenames from release/manifest payloads in one shared helper

def get_diff_sidecar(payload):
	# Return None when payload is not a mapping-like object
	if not isinstance(payload, dict): return None
	# Read top-level diff field from payload
	diff = payload.get('diff')
	# Return nested sidecar filename when new diff object shape is present
	if isinstance(diff, dict):
		# Return sidecar only when value is a string filename
		if isinstance(diff.get('sidecar'), str): return diff.get('sidecar')
		# Return None when nested sidecar is absent
		return None
	# Return legacy direct diff filename when payload still stores a string
	if isinstance(diff, str): return diff
	# Return None when diff field has unsupported shape
	return None
