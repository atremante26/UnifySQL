import json
import sys

import requests

API = "http://localhost:5001"
GOLDEN_PATH = "unifysql/eval/golden_set.json"

DB_IDS = [
    "battle_death",
    "concert_singer",
    "course_teach",
    "cre_Doc_Template_Mgt",
    "dog_kennels",
    "employee_hire_evaluation",
    "flight_2",
    "network_1",
    "orchestra",
    "pets_1",
    "student_transcripts_tracking",
    "voter_1",
    "wta_1",
    "singer",
]


def main() -> int:
    schema_ids: dict[str, str] = {}
    failed: list[str] = []

    for db_id in DB_IDS:
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
        print(f"{db_id}: {body['schema_id']} ({body['status']})")

    if failed:
        print(f"\n{len(failed)} registration(s) failed: {', '.join(failed)}")
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

    print(f"\nUpdated {updated}/{len(golden)} golden set entries")
    if unmatched:
        print("No schema_id mapping for:")
        for m in unmatched:
            print(f"  {m}")
        return 1

    return 0


if __name__ == "__main__":
    sys.exit(main())
