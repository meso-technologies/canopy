# Enum helpers used by canopy
import enum

# Taxon rank enum used to generate DuckDB enum values in fuse stage
class TaxonRank(enum.Enum):
	KINGDOM = 0
	SUBKINGDOM = 1
	PHYLUM = 2
	SUBPHYLUM = 3
	CLASS = 4
	SUBCLASS = 5
	ORDER = 6
	SUBORDER = 7
	FAMILY = 8
	SUBFAMILY = 9
	TRIBE = 10
	SUBTRIBE = 11
	GENUS = 12
	SUBGENUS = 13
	SECTION = 14
	SUBSECTION = 15
	SERIES = 16
	SUBSERIES = 17
	SPECIES = 18
	GREX = 19
	SUBSPECIES = 20
	VARIETY = 21
	SUBVARIETY = 22
	FORM = 23
	SUBFORM = 24
	LUSUS = 25
	CULTIVAR = 26
	COMPLEX = 27
	UNRANKED = 28
