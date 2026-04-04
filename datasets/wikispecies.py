#
#		Wikispecies Data
#

from ..utils.log import mesologger
from ..utils.filehandlers import fetch

# Name and default URL
sources = [{
	"name": "wikispecies-pages",
	"url": "https://dumps.wikimedia.org/specieswiki/latest/specieswiki-latest-page.sql.gz"
},{
	"name": "wikispecies-categorylinks",
	"url": "https://dumps.wikimedia.org/specieswiki/latest/specieswiki-latest-categorylinks.sql.gz"
},{
	"name": "wikispecies-templatelinks",
	"url": "https://dumps.wikimedia.org/specieswiki/latest/specieswiki-latest-templatelinks.sql.gz"
}]

async def update_wikispecies(session):
	mesologger.info(f"############### Starting Wikispecies Update  ###############")
	# Multiple files needed
	for source in sources:
		# See if we have a new remote version vs. our latest processed
		if await fetch(session, source): pass