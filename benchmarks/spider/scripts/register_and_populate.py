import argparse
import json
import sys
import time
from pathlib import Path

import requests

API = "http://localhost:5001"
GOLDEN_PATH = "unifysql/eval/golden_set.json"
SCHEMA_IDS_PATH = "benchmarks/spider/schema_ids.json"
DATABASE_DIR = "benchmarks/spider/database"

# Databases skipped on purpose — keep in sync with EXCLUDED_DBS in load_spider.sh.
EXCLUDED_DBS = {
    "car_1",  # pgloader types join columns inconsistently (bigint vs text), breaking gold SQL joins
}


def discover_db_ids() -> list[str]:
    return sorted(
        entry.name
        for entry in Path(DATABASE_DIR).iterdir()
        if entry.is_dir() and entry.name not in EXCLUDED_DBS
    )


def wait_for_semantic_layers(
    schema_ids: dict[str, str], timeout: float, poll_interval: float = 5.0
) -> list[str]:
    """
    Polls GET /semantic-layer/{schema_id} until each schema's async annotation
    pipeline (POST /schemas returns 202 and annotates in a background thread)
    has stored its semantic layer. Returns db_ids not ready within timeout.
    """
    if not schema_ids:
        return []

    pending = dict(schema_ids)
    deadline = time.monotonic() + timeout
    while pending and time.monotonic() < deadline:
        for db_id, schema_id in list(pending.items()):
            try:
                resp = requests.get(f"{API}/semantic-layer/{schema_id}", timeout=30)
            except requests.RequestException:
                break
            if resp.status_code == 200:
                print(f"{db_id}: semantic layer ready")
                del pending[db_id]
        if pending:
            time.sleep(poll_interval)

    if not pending:
        # Table embeddings are written right after the layer is stored and have
        # no status endpoint; give the last schema's write a moment to land.
        time.sleep(10)
    return sorted(pending)


def main() -> int:
    parser = argparse.ArgumentParser(
        description=(
            "Register loaded spider_* databases with the UnifySQL API and record "
            "the db_id -> schema_id mapping in schema_ids.json. Registration "
            "triggers paid LLM annotation per new schema, so db_ids already in "
            "the mapping are skipped unless --force is given."
        )
    )
    parser.add_argument(
        "db_ids",
        nargs="*",
        help=f"Subset of db_ids to register (default: all under {DATABASE_DIR}/).",
    )
    parser.add_argument(
        "--force",
        action="store_true",
        help="Re-register db_ids even if they already have a schema_id.",
    )
    parser.add_argument(
        "--wait-timeout",
        type=float,
        default=600.0,
        help="Seconds to wait for async annotation to finish (default: 600).",
    )
    args = parser.parse_args()

    db_ids = args.db_ids or discover_db_ids()

    if Path(SCHEMA_IDS_PATH).exists():
        with open(SCHEMA_IDS_PATH) as f:
            schema_ids: dict[str, str] = json.load(f)
    else:
        schema_ids = {}

    failed: list[str] = []
    skipped: list[str] = []
    newly_registered: dict[str, str] = {}

    for db_id in db_ids:
        if db_id in schema_ids and not args.force:
            skipped.append(db_id)
            continue

        pg_db = f"spider_{db_id.lower()}"
        try:
            resp = requests.post(
                f"{API}/schemas",
                json={
                    "connection_string": (
                        f"postgresql://postgres:postgres@postgres:5432/{pg_db}"
                    ),
                    "dialect": "postgres",
                },
                timeout=60,
            )
        except requests.RequestException as e:
            print(f"FAIL {db_id}: request error — {e}")
            failed.append(db_id)
            continue

        if resp.status_code not in (200, 202):
            print(f"FAIL {db_id}: HTTP {resp.status_code} — {resp.text}")
            failed.append(db_id)
            continue

        body = resp.json()
        schema_ids[db_id] = body["schema_id"]
        newly_registered[db_id] = body["schema_id"]
        print(f"{db_id}: {body['schema_id']} ({body['status']})")

    # Persist the mapping even on partial failure so successful (paid)
    # registrations are never repeated on re-run.
    with open(SCHEMA_IDS_PATH, "w") as f:
        json.dump(schema_ids, f, indent=2, sort_keys=True)
        f.write("\n")

    print(
        f"\n{len(schema_ids)} schema_id(s) in {SCHEMA_IDS_PATH} "
        f"({len(skipped)} skipped as already registered)"
    )

    not_ready = wait_for_semantic_layers(newly_registered, timeout=args.wait_timeout)
    if not_ready:
        print(f"{len(not_ready)} schema(s) never became ready: {', '.join(not_ready)}")
        print(
            "Check API logs (the annotation pipeline swallows errors); "
            "re-register with --force if the pipeline failed."
        )
        failed.extend(not_ready)

    if failed:
        print(f"{len(failed)} registration(s) failed: {', '.join(failed)}")
        print("Not patching golden_set.json — fix registrations and re-run.")
        return 1

    # Patch golden_set.json
    with open(GOLDEN_PATH) as f:
        golden = json.load(f)

    updated, unmatched = 0, []
    for entry in golden:
        schema_id = schema_ids.get(entry["db_id"])
        if schema_id:
            entry["schema_id"] = schema_id
            updated += 1
        else:
            unmatched.append(f"{entry['question_id']} ({entry['db_id']})")

    with open(GOLDEN_PATH, "w") as f:
        json.dump(golden, f, indent=2)

    print(f"Updated {updated}/{len(golden)} golden set entries")
    if unmatched:
        print("No schema_id mapping for:")
        for m in unmatched:
            print(f"  {m}")
        return 1

    return 0


if __name__ == "__main__":
    sys.exit(main())
