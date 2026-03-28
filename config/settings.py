# Runtime settings for canopy pipeline
import uuid

# Import canopy credentials from local secrets file
from .secrets import GBIF_USER, GBIF_PASSWORD, WIKIDATA_TOKEN

# Base canopy runtime defaults
class Base:
	# Keep naming for compatibility with existing logging
	NAME = 'canopy'
	# Keep deterministic UUID namespace for id hashing
	HASH_NAMESPACE = uuid.uuid5(uuid.NAMESPACE_URL, 'www.meso.cloud')
	# Default runtime behavior
	DEBUG = False
	VERBOSE = False
	FORCE = False
	CSV = False
	# Download checks enabled by default
	CHECK_FOR_DOWNLOADS = True
	# Download-only orchestration mode
	DOWNLOAD_ONLY = False
	# Full run by default
	BACKBONE_LOOPS = 0
	# Optional API credentials
	GBIF_USER = GBIF_USER
	GBIF_PASSWORD = GBIF_PASSWORD
	WIKIDATA_TOKEN = WIKIDATA_TOKEN

# Fast partial-data profile for local development
class Debug(Base):
	DEBUG = True
	BACKBONE_LOOPS = 10000

# Settings proxy used by canopy modules
class Settings:
	def __init__(self): self._instance = None
	def __getattr__(self, name):
		if self._instance is None: raise RuntimeError('Canopy settings not initialized - did you run canopy.run?')
		return getattr(self._instance, name)
	def __str__(self):
		if self._instance is None: return 'CanopySettings(uninitialized)'
		return str(self._instance)
	@property
	def __dict__(self): return vars(self._instance)
	def set_config(self, config): self._instance = config

# Build runtime settings object from canopy CLI args
def build_settings(args):
	# Pick profile class
	profile = Debug if bool(getattr(args, 'debug', False)) else Base
	# Build runtime subclass so we can overlay CLI flags cleanly
	class Runtime(profile):
		# Overlay generic CLI flags
		VERBOSE = bool(getattr(args, 'verbose', False)) or profile.VERBOSE
		FORCE = bool(getattr(args, 'force', False)) or profile.FORCE
		CSV = bool(getattr(args, 'csv', False)) or profile.CSV
	return Runtime

# Export settings proxy
settings = Settings()
