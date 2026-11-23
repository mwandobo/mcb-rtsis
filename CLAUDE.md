# mcb-rtsis — Working Rules

## Never alter BOT-validated query logic directly

The files in `sqls/` are the source-of-truth queries behind BOT (Bank of
Tanzania) regulatory submissions. Many have already passed BOT validation.
**When adding a feature to a pipeline (incremental extraction, a new field,
etc.), never edit a `.sql` file's SELECT/JOIN/CASE logic in place.**

Instead:

- Incremental (watermark) extraction is added only in the pipeline's Python
  wrapper, in a `get_<name>_query(self, last_timestamp=None)` method: read
  the `.sql` file verbatim, and *conditionally append* an
  `AND <table>.TMSTAMP > '<watermark>'` clause only when `last_timestamp` is
  set. Parenthesize the original WHERE predicate before appending, so an
  unparenthesized `OR` in the original query isn't silently altered by
  operator precedence.
- On a full-load run (`--full-load`, or the first run with no prior
  successful run), `last_timestamp` is `None`/falsy, so the query returned
  is byte-identical to the original `.sql` file — same columns, joins, and
  CASE/lookup mappings as what already passed BOT validation.
- If a pipeline's driver table has no genuine last-modified column (a
  business-event date doesn't count, and a column only reachable via an
  unsafe LEFT JOIN doesn't count either), don't force an incremental filter
  onto it. Leave it as a full scan every cycle, relying on the existing
  `ON CONFLICT ... DO UPDATE` upsert for correctness (e.g.
  `directors_records`, `account_product_category`, `account_information`).

Why: a `.sql` file edited in place for one feature can silently drift from
the exact logic BOT already validated, with no git-visible warning unless
someone checks. On 2026-11-20, `git diff` on `sqls/` turned up several files
(`personal-data-corporates-v4.sql`, `other_assets.sql`, and others) with
substantial uncommitted content changes unrelated to any pipeline feature,
sitting unnoticed in the working tree. Keeping the append-only Python-layer
pattern means `git diff -- sqls/` should normally be empty after
incremental-extraction work, which makes genuine drift easy to spot.

## Git commit messages

Do not add a Claude/Anthropic attribution signature to commit messages in
this repo.

## backend/.env

Contains live DB2/Postgres credentials and the JWT secret. It was tracked in
git despite being listed in `.gitignore` (it was committed before that rule
was added). As of 2026-11-20 it has been untracked (`git rm --cached`) so
`.gitignore` now actually takes effect. Do not re-add it to git.

## The actual BOT RTSIS submission mechanism is not in this repo

This repo only contains the DB2->Postgres ETL layer and an internal
monitoring dashboard. The real submission to Bank of Tanzania (a REST API
the bank hosts, per `Annexures-v1/Annexure 2_openapi_banks Ver03.yaml`, that
BOT's RTSIS calls into) is not implemented anywhere in this codebase. If a
data-quality issue (duplicates, stale values) is found in a Postgres table
that feeds a BOT report, do not assume it's contained to internal staging -
whether it already reached BOT depends on that external system's own
behavior, which can't be checked from here. Flag this explicitly rather than
reassuring that it's safe.

## `nest start --watch` kills running pipelines

The backend dev server (`npm run start:dev`, i.e. `nest start --watch`)
tree-kills its *entire* child-process tree on every restart, including any
Python pipeline processes spawned via `child_process.spawn`. Any backend
TypeScript edit while pipelines are running continuously will kill all of
them as a side effect, not just the one being edited. After editing backend
files, check `GET /api/stats` for `runningPipelines` and restart whichever
pipelines were running before the edit.
