# ============================================================
# A/B Test: Raw occurrence expansion vs habitat rollup
# ============================================================
#
# QUESTION: Should we expand raw occurrences to parent taxa (before habitat)
#           or roll up habitat tile counts (after habitat)?
#
# RESULTS:
#   Approach A (expand raw occurrences):
#     528M × ~5 levels = 2,512,934,075 additional rows = 3.0B total
#     ~78GB memory for GEOMETRY columns
#     VERDICT: Infeasible on any reasonable hardware
#
#   Approach B (roll up habitat_points):
#     41M tiles → 193M ancestor rows → re-aggregated to ~33M new tiles
#     Rollup took 8-30s depending on approach
#     VERDICT: Fast and correct
#
# GOTCHAS:
#   - The ancestor_map (3.6M edges, all levels flattened) causes double-counting
#     if used for rollup directly. A family would get species points both directly
#     AND through genus. Must use direct parentage (717k edges) and iterate levels.
#   - Rollup via recursive level-by-level parent walking is correct:
#     Level 1: genus = concat(species children)
#     Level 2: family = concat(genus children, which already include species)
#     Typically 4-5 levels until no new parents found.
#
# LEARNINGS:
#   - 528M raw occurrences × 5 ancestor levels = always OOM
#   - Habitat tile aggregation compresses 528M → 41M (13x), making rollup feasible
#   - The polars list.eval approach later eliminated even this expansion entirely
#
# Run: .venv/Scripts/python -X utf8 abtest/01_expansion_approaches.py

import duckdb, time

RELEASE = 'data/releases/20260328-ac238ab708c1/20260328-ac238ab708c1.parquet'
OCCURRENCES = 'data/geo/occurrences.parquet'
TMP = 'data/temp'

def main():
	t0 = time.time()
	db = duckdb.connect(':memory:')
	db.execute(f"SET temp_directory = '{TMP}'")
	db.execute("INSTALL spatial; LOAD spatial;")

	# Load raw occurrences
	print("Loading occurrences...", flush=True)
	t = time.time()
	db.execute(f"""
		CREATE TABLE occurrences AS SELECT taxon, location, elevation FROM '{OCCURRENCES}'
		WHERE NOT spatial_issue AND NOT ST_Equals(location, ST_Point(0, 0)) AND NOT ST_Equals(location, ST_Point(1, 1));
	""")
	raw_cnt = db.execute("SELECT COUNT(*) FROM occurrences").fetchone()[0]
	print(f"[{time.time()-t:.0f}s] {raw_cnt:,} raw rows")

	# Build ancestor mapping
	t = time.time()
	db.execute(f"""
		CREATE TABLE ancestors AS
		SELECT s.gbif_id,
			g.gbif_id as genus_gbif, f.gbif_id as family_gbif,
			o.gbif_id as order_gbif, c.gbif_id as class_gbif, p.gbif_id as phylum_gbif
		FROM '{RELEASE}' s
		LEFT JOIN '{RELEASE}' g ON s.genus = g.name_consensus AND g.rank_consensus = 'GENUS' AND g.accepted AND g.gbif_id IS NOT NULL
		LEFT JOIN '{RELEASE}' f ON s.family = f.name_consensus AND f.rank_consensus = 'FAMILY' AND f.accepted AND f.gbif_id IS NOT NULL
		LEFT JOIN '{RELEASE}' o ON s."order" = o.name_consensus AND o.rank_consensus = 'ORDER' AND o.accepted AND o.gbif_id IS NOT NULL
		LEFT JOIN '{RELEASE}' c ON s.class = c.name_consensus AND c.rank_consensus = 'CLASS' AND c.accepted AND c.gbif_id IS NOT NULL
		LEFT JOIN '{RELEASE}' p ON s.phylum = p.name_consensus AND p.rank_consensus = 'PHYLUM' AND p.accepted AND p.gbif_id IS NOT NULL
		WHERE s.accepted AND s.gbif_id IS NOT NULL;
	""")
	print(f"[{time.time()-t:.0f}s] Ancestor mapping ready")

	# Estimate expansion sizes — DON'T actually create the table
	t = time.time()
	expand_est = db.execute("""
		SELECT 
			SUM(CASE WHEN genus_gbif IS NOT NULL THEN 1 ELSE 0 END) as genus_occ,
			SUM(CASE WHEN family_gbif IS NOT NULL THEN 1 ELSE 0 END) as family_occ,
			SUM(CASE WHEN order_gbif IS NOT NULL THEN 1 ELSE 0 END) as order_occ,
			SUM(CASE WHEN class_gbif IS NOT NULL THEN 1 ELSE 0 END) as class_occ,
			SUM(CASE WHEN phylum_gbif IS NOT NULL THEN 1 ELSE 0 END) as phylum_occ
		FROM occurrences o JOIN ancestors a ON o.taxon = a.gbif_id
	""").fetchone()
	total_expand = sum(v for v in expand_est if v)
	print(f"[{time.time()-t:.0f}s] Expansion estimate: {total_expand:,} additional rows")
	print(f"  genus: {expand_est[0]:,}, family: {expand_est[1]:,}, order: {expand_est[2]:,}")
	print(f"  class: {expand_est[3]:,}, phylum: {expand_est[4]:,}")
	print(f"  Total with originals: {raw_cnt + total_expand:,} (~{(raw_cnt + total_expand) * 26 / 1e9:.0f}GB)")

	print(f"\n=== TOTAL: {time.time()-t0:.0f}s ===")
	db.close()

if __name__ == '__main__':
	main()
