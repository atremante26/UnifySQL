# Spider Benchmark Scripts

Two entry points, run in this order: `load_spider.sh`, then `register_and_populate.py`.

## `load_spider.sh`

- Loads every Spider SQLite database under `benchmarks/spider/database/*/` into the
Docker Postgres container (`docker-postgres-1`, host port 5433) as `spider_<db_id>`
(lowercased), via pgloader. 

- Each SQLite file is first copied to `/tmp/spider_load/` and sanitized there (the 
originals are never modified), then loaded and verified with a source-vs-target 
row-count comparison per table. 

- Databases that cannot load correctly are listed in `EXCLUDED_DBS` with a documented 
reason each.

## `register_and_populate.py`

- Registers loaded `spider_*` databases as schemas with the running UnifySQL API
(`POST /schemas`), records the `db_id -> schema_id` mapping in
`benchmarks/spider/schema_ids.json` (read by the eval harness for spider-mode
runs), and patches the `schema_id`s into the eval golden set.

- With no arguments it discovers every database under `benchmarks/spider/database/`
(minus `EXCLUDED_DBS`); pass db_ids to register a subset.

- Registration triggers paid LLM annotation for every new schema, which is why this
step is deliberately separate from loading. db_ids already in `schema_ids.json`
are skipped on re-run unless `--force` is given.

- Annotation runs asynchronously in the API, so the script polls
`GET /semantic-layer/{schema_id}` until every new schema is ready
(`--wait-timeout`, default 600s) before exiting — do not start an eval until it
exits 0, or the harness races the annotation pipeline.

- Run it only after a successful load, and only when you actually need schemas 
(re)registered, not on every reload.

## `sanitize_sqlite.py`

- Internal helper invoked by `load_spider.sh` on the staged copy of each
database. 

- Fixes Spider schemas and data that pgloader or Postgres rejects:
    - Declared types pgloader cannot parse (`varchar2`, `number(p,s)`, `... unsigned`)
are rewritten in the staged schema
    - Non-numeric text and empty strings in numeric-declared columns become NULL
    - Zero dates (`0000-00-00`) in date columns become NULL (or the row is dropped when 
the column is NOT NULL)
    - Invalid UTF-8 bytes in text values are replaced.
