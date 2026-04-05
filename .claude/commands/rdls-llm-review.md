# RDLS LLM Review

Run the LLM-assisted HEVL classification pipeline to fix content-blind over-classification.

## Input
$ARGUMENTS - options: `--dist-dir PATH`, `--metadata-dir PATH`, `--output-dir PATH`, `--dry-run`, `--max-records N`, or just a question about LLM review results

## Instructions

### If running the pipeline:

1. Check prerequisites:
   - `ANTHROPIC_API_KEY` environment variable must be set
   - RDLS distribution files in `--dist-dir` (from hdx-metadata-crawler output)
   - HDX dataset metadata JSONs in `--metadata-dir`
   - Column cache in `output/column_cache/` (optional but saves time)

2. Review config at `configs/llm_review.yaml`:
   - `llm.max_cost_usd` - cost guardrail (default $15)
   - `llm.model` - must be `claude-haiku-4-5-20251001`
   - `merge.llm_overrides_signals` - whether LLM wins on disagreement

3. Run the pipeline:
   ```bash
   python -m src.llm_review \
     --dist-dir path/to/rdls/dist \
     --metadata-dir path/to/dataset_metadata \
     --output-dir output/llm \
     --config configs/llm_review.yaml
   ```

4. For testing, use `--dry-run` (no LLM calls) or `--max-records N` (limit)

5. Monitor progress:
   - Phase 1 (triage): instant - prints bucket counts
   - Phase 2 (columns): uses cache if available, otherwise slow (0.5s/resource)
   - Phase 3 (LLM): ~22 min for 12K records at $7, cached responses reused
   - Phase 4 (merge): instant - prints reclassification counts

### If inspecting results:

1. Check `review_report.csv` in the output directory:
   - Columns: `rdls_id`, `new_id`, `old_rdt`, `new_rdt`, `is_rdls`, `confidence`, `changes`
   - Filter for `changes != ""` to see reclassified records

2. Check output directories:
   - `output/llm/dist/` - valid RDLS records (tiered: high, medium, low)
   - `output/llm/not_rdls/` - records classified as non-RDLS by LLM
   - `output/llm/invalid/` - records that fail schema validation

3. For a specific record, check its LLM cache:
   - Find the prompt hash in `review_report.csv`
   - Look up `output/llm_review/cache/{hash}.json` for the full LLM response with reasoning

### If resuming after interruption:

- LLM cache persists - re-running costs $0 for already-classified records
- Column cache persists - no re-fetching needed
- Use same `--output-dir` to resume; Phase 3 skips cached records automatically
