# Main local storage for canopy pipeline
import os

# Get canopy package base dir
CANOPY_DIR = os.path.dirname(os.path.abspath(__file__))
# Resolve canopy data dir from env override or local package dir
DATA_DIR = os.environ.get('CANOPY_DATA_DIR', os.path.join(CANOPY_DIR, 'data'))

# Define all canopy subdirectories
SRC_DIR = os.path.join(DATA_DIR, 'source')
TMP_DIR = os.path.join(DATA_DIR, 'temp')
PROCESSED_DIR = os.path.join(DATA_DIR, 'processed')
RELEASES_DIR = os.path.join(DATA_DIR, 'releases')
API_DATA_DIR = os.path.join(DATA_DIR, 'apis')
GEO_DIR = os.path.join(DATA_DIR, 'geo')

# Import canopy runtime settings proxy and builder
from .config.settings import settings, build_settings
