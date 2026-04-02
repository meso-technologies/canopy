# 06 Geo rework findings

## Scope

This note condenses the geo rework A B tests that were run during the geo cutover session and records what to keep, what to avoid, and why.

## Final production direction

- Keep one production geo path behind `--geo` only.
- Keep one final release artifact output: `geo.<YYYYMMDD>.parquet`.
- Use polars for load, rollup, and habitat tile aggregation.
- Use FAISS for centroid selection with legacy selection policy.
- Package in DuckDB and normalize centroid payload in SQL.

## What was tested

### 1) Accepted taxa filtering in load stage

Compared on real data:

- Python set filter: `taxon.is_in(set(...))`
- Lazy join filter: `join(accepted_lf, on='taxon')`
- Both with and without `collect(engine='streaming')`

Observed on full 528M occurrence parquet + release `20260328-ac238ab708c1`:

- `load_set auto`: ~111s, high peak memory
- `load_set streaming`: ~113s, lower peak memory
- `load_join auto`: ~74s, very high peak memory
- `load_join streaming`: ~31s, fast and controlled memory

Decision:

- Use lazy join + streaming collect in production load path.

### 2) Rollup unseen parent detection

Compared:

- Python set membership per level
- DataFrame anti join per level

Observed on full run:

- Python set path was slightly faster and lower memory in this workload.

Decision:

- Keep Python set approach in rollup for now.

### 3) Centroid input strategy

Compared against raw baseline FAISS behavior on stratified ~50k taxa sample:

- Baseline: raw occurrence arrays
- Candidate linear: habitat center arrays + `weights=count`
- Candidate sqrt: habitat center arrays + `weights=sqrt(count)`

Observed:

- Habitat weighted variants were faster than raw baseline.
- Linear weights matched baseline better than sqrt in distance metrics.

Decision:

- Use habitat centers with linear weights for centroid clustering.

### 4) Parallel clustering speedups

Compared on ~49k comparable taxa:

- Optimized single process UDF style run
- Parallel imap worker pool
- Parallel shard worker pool

Observed:

- Parallel imap gave the best speedup (~2x vs optimized single process in the sample run).

Decision:

- Use multiprocessing worker pool for centroid stage.

### 5) Python dedupe vs SQL dedupe parity

Question:

- Does removing Python pre dedupe change final centroid coordinates?

Tested:

- Python pre dedupe + legacy SQL normalization
- SQL normalization only

Legacy SQL normalization used:

- `list_distinct(list_transform(centroids, coord -> [coord[1]::DECIMAL(8,4), coord[2]::DECIMAL(8,4)]))::JSON`

Observed on 50k taxa:

- 0 normalized centroid mismatches.

Decision:

- Remove Python centroid dedupe.
- Keep dedupe and coordinate normalization in DuckDB packaging SQL.

## Keep vs remove

Keep for posterity:

- This summary file: `06_geo_rework_findings.md`
- Consolidated executable benchmark: `07_geo_centroid_bench.py`
- Rollup and habitat benchmark: `08_rollup_habitat_abtest.py`
- Packaging profiler benchmark: `09_packaging_profile.py`

Remove superseded one off scripts from this session:

- `06_assumption_bench.py`
- `06_assumption_bench_isolated.py`
- `08_centroid_speed_experiments.py`
- `09_dedupe_equivalence.py`
- `11_packaging_abtest.py`
+
+## Additional learnings from follow-up tests (08 to 09)
+
+### 6) Packaging hotspot root cause (09)
+
+Measured with real dump inputs:
+
+- habitat rows: ~70,224,662
+- habitat taxa: ~443,972
+- centroids: ~443,972
+- elevation bins: ~3,066,068
+
+Observed timing pattern:
+
+- The dominant cost is habitat aggregation:
+  - `array_agg(struct_pack(... ) ORDER BY ...) GROUP BY gbif_id`
+- Centroid clean/profile clean/update steps are comparatively small.
+
+Decision:
+
+- Treat habitat aggregation as the primary optimization and memory-risk target.
+
+### 7) Packaging variant outcomes (09)
+
+Compared:
+
+- A: current create + alter + updates shape
+- C: pre-aggregate and single CTAS join
+- D: staged table flavor of join path
+
+Observed on full runs:
+
+- A was fastest in this workload.
+- C was significantly slower.
+- D was better than C but still slower than A.
+
+Decision:
+
+- Keep A semantics for speed.
+- Improve reliability via memory lifecycle and partitioning, not by switching to C.
+
+### 8) Dead ends and anti-patterns to avoid
+
+- Broad `json_each` reconstruction from final geo JSON for large A/B prep caused heavy spill storms and unstable runs.
+- Long inline benchmark commands without durable per-step logging made failures harder to interpret.
+- Wrapper launches with `uv --no-sync` fail when canopy env is newly created or incompatible; run `uv sync --project importer/canopy` first.
+
+### 9) What worked reliably
+
+- Durable step logging benchmark (`09_packaging_profile.py`) with partition mode for fast hotspot detection.
+- Spooling stage payloads to parquet before high-pressure phases.
+- Running canopy with a healthy synced project env restored normal startup behavior.

## Operational guidance

- Keep abtest scripts and checkpoints in `abtest/` and `data/temp/` only.
- Do not leave production geo flow with debug flags or alternate debug control paths after cutover.
- For production validation, run one forced geo build and compare resulting release artifact metrics against prior known good release.
