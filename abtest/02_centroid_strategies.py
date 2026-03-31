# ============================================================
# A/B Test: Centroid computation strategies
# ============================================================
#
# QUESTION: Can we replace the FAISS UDF with pure SQL or simpler approaches?
#
# RESULTS (10k sample species):
#   Pure SQL top-3 by count (no distance filter):
#     Speed: 6s for 679k taxa (instant)
#     Quality: p50=0.31° from production, but clusters geographically —
#              all 3 centroids often in one dense region for wide-range species
#     VERDICT: Fast but poor diversity for species with 100+ tiles
#
#   SQL density × distance weighting:
#     Speed: 13s for 679k taxa
#     C1 picks highest density, C2 maximizes count*sqrt(dist_from_C1), etc
#     Quality: p50=0.36° from production, much better geographic spread
#     VERDICT: Good approximation but not identical to k-means
#
#   FAISS with weights= on habitat centers (linear):
#     Speed: ~182 taxa/s → ~63 min for 687k taxa
#     Quality: p50=0.005° from production — near-identical
#     VERDICT: Best quality, same speed as current approach
#
#   FAISS with weights= on habitat centers (sqrt):
#     Quality: p50=0.010° — slightly worse than linear
#     VERDICT: Linear weights best match production (which implicitly weights by density)
#
#   FAISS with weights= on habitat centers (log):
#     Quality: p50=0.012° — worst of the three
#     VERDICT: Over-dampens density signal
#
# GOTCHAS:
#   - Production FAISS clusters RAW occurrences. 50k occurrences in one tile = 50k points
#     pulling k-means toward that location. This is IMPLICIT weighting by density.
#     Using habitat tile centers WITHOUT weights loses this. weights= on tile counts restores it.
#   - FAISS k-means p50 centroid distance between habitat-center and raw-occurrence clustering
#     is only 0.005° (500m). The tile quantization dominates, not the k-means convergence.
#   - For species with ≤3 tiles, FAISS just returns all points — no k-means. 49% of species.
#   - The FAISS UDF bottleneck is per-batch Python overhead (2048 taxa per batch), not per-point
#     FAISS computation. Feeding fewer points per taxon doesn't help much.
#
# LEARNINGS:
#   - Don't try to replace FAISS with SQL heuristics — years of edge-case tuning in the
#     clustering logic (adaptive k, min_points_per_centroid, diversity selection, range_search)
#     can't be replicated in a few SQL queries
#   - Linear weights on habitat center_points is the correct FAISS approach — it faithfully
#     reconstructs the implicit density weighting of raw occurrence clustering
#   - The UDF shape (parallel arrays vs struct) matters: parallel arrays are 9% faster
#     because Arrow chunked array conversion is faster than struct deserialization
#
# UDF SHAPE COMPARISON (50k taxa):
#   Shape A (parallel FLOAT[] x, y, w):  190 taxa/s  ← WINNER
#   Shape B (STRUCT(lng[], lat[], w[])):  175 taxa/s
#   Shape C (FLOAT[2][] pairs):          FAILED (ragged array numpy conversion)
#
# This file is reference only — the FAISS UDF stays unchanged.
# The improvement is feeding it habitat center_points instead of raw occurrences,
# with weights= for density-aware clustering.
