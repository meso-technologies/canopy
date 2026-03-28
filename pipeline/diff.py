#
#	Collect and produce manifest files that include the difference to our last published version as changelog
#

# Main flow
def diff(results):
	print(f"IMPORT : ############### Producing Release Diff ###############")
	# TODO: Remove all files older than our published version