# Registers all 14 Spider databases with UnifySQL and prints schema_ids

API="http://localhost:5001"

DB_IDS=(
    "battle_death"
    "car_1"
    "concert_singer"
    "course_teach"
    "cre_Doc_Template_Mgt"
    "dog_kennels"
    "employee_hire_evaluation"
    "flight_2"
    "network_1"
    "orchestra"
    "pets_1"
    "student_transcripts_tracking"
    "voter_1"
    "wta_1"
)

for db_id in "${DB_IDS[@]}"; do
    pg_db="spider_$(echo $db_id | tr '[:upper:]' '[:lower:]')"
    echo -n "$db_id: "
    curl -s -X POST $API/schemas \
      -H "Content-Type: application/json" \
      -d "{
        \"connection_string\": \"postgresql://postgres:postgres@postgres:5432/$pg_db\",
        \"dialect\": \"postgres\"
      }" | python3 -c "import sys,json; d=json.load(sys.stdin); print(d['schema_id'])"
done