# Workflow: publishing v1.0 records to JKAN

This is the general procedure for publishing a batch of validated RDLS v1.0 records to the [GFDRR/rdl-jkan](https://github.com/GFDRR/rdl-jkan) catalogue. It applies to any collection, not a specific upload.

## How JKAN ingests a record

JKAN (a Jekyll site) needs, per dataset, a JSON file in `_datasets/json/` **and** a Markdown stub in `_datasets/`. You only ever commit the **JSON** - the repo's `pr-sync` GitHub Action runs `python main.py --ci --markdown --vectors` on each PR and commits the generated `.md` stubs and vector embeddings back onto the PR branch. So a PR is just "add JSON files to `_datasets/json/`".

- **Base branch:** `rdl-1.0`
- **One new branch per PR** (the catalogue's contribution rule), e.g. `upload/<nn>-<slug>`.
- **PRs are additive and independent** - different files, so they merge in any order without conflict.

## Why batches

A large corpus is split into reviewable PRs, ordered so coherent collections land first and the largest source (HDX) is chunked into fixed-size batches (e.g. 500 records, sorted by id). Each batch is one branch and one PR. Counts are v1.0-only: any v0.3 `*_v03.json` companion files kept alongside are excluded.

## Two-phase rollout (stage now, submit later)

A "draft PR" on GitHub is a live, open PR - not a saved draft. To prepare ahead of approval without opening 23 live PRs (and firing 23 CI runs) prematurely, stage the branches first and open the PRs when the team gives the green light.

1. **Stage** - push each batch as its own branch off `rdl-1.0` (copies the batch's JSON into `_datasets/json/`, commits, pushes). No PR, no CI.
2. **Open** - when approved, open a normal (ready-for-review) PR from each staged branch. Opening is a GitHub-API/web action, so it works from a phone (a pre-filled `compare` URL) or a computer.

Reviewers approve; merging is a final click anyone with rights can do.

## Tooling

The generated upload kit lives under `temp/upload/` for a given run:

- `gen_upload_manifests.py` - builds per-batch file manifests + a batch index from `output/`.
- `stage_jkan_branches.ps1 -Batch <n|all>` - phase 1 (stage branches; no PR).
- `open_jkan_pr.ps1 -Batch <n|all>` - phase 2 (open PRs; `-Draft` optional).
- `open_pr_links.md` - tap-to-open `compare` URLs for opening PRs from a phone.

## Gotchas

- **Do not keep the rdl-jkan repo inside a cloud-sync folder** (OneDrive/Dropbox): the sync client locks `.git` during git's auto-gc and causes `index.lock` rename failures. If you must, set `git config gc.auto 0` and exclude the repo from antivirus real-time scanning.
- Run git on the repo from one machine at a time; let any sync settle before switching.
- `gh` must be authenticated (`gh auth login`) and the PR commands target the repo explicitly (`--repo GFDRR/rdl-jkan`) since the repo has more than one remote.
