CONTAINER="docker-postgres-1"
PG_HOST="localhost"
PG_PORT="5433"
PG_USER="postgres"
PG_PASS="postgres"
BASE_DIR="benchmarks/spider/database"

DB_IDS=(
    "battle_death"
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
    "singer"
)

FAILED=()

echo "Loading ${#DB_IDS[@]} Spider databases via pgloader..."

for db_id in "${DB_IDS[@]}"; do
    echo ""
    echo "  Processing $db_id..."

    sqlite_file="$BASE_DIR/$db_id/$db_id.sqlite"

    if [ ! -f "$sqlite_file" ]; then
        echo "  FAIL $db_id — sqlite file not found at $sqlite_file"
        FAILED+=("$db_id")
        continue
    fi

    # Use lowercase for Postgres database name to avoid case sensitivity issues
    pg_db="spider_$(echo $db_id | tr '[:upper:]' '[:lower:]')"

    # Drop and recreate database
    if ! docker exec $CONTAINER psql -U $PG_USER \
        -c "DROP DATABASE IF EXISTS $pg_db;" > /dev/null 2>&1; then
        echo "  FAIL $db_id — could not drop $pg_db (container up? name correct?)"
        FAILED+=("$db_id")
        continue
    fi
    docker exec $CONTAINER psql -U $PG_USER \
        -c "CREATE DATABASE $pg_db;" > /dev/null 2>&1

    # Load via pgloader
    pgloader_output=$(pgloader --quiet "$sqlite_file" \
        "postgresql://$PG_USER:$PG_PASS@$PG_HOST:$PG_PORT/$pg_db" 2>&1)

    if [ $? -eq 0 ]; then
        echo "  SUCCESS $db_id loaded: $pg_db"
    else
        echo "  FAIL $db_id — pgloader error:"
        echo "$pgloader_output" | tail -5 | sed 's/^/    /'
        FAILED+=("$db_id")
    fi
done

echo ""
if [ ${#FAILED[@]} -eq 0 ]; then
    echo "Done — all ${#DB_IDS[@]} databases loaded."
else
    echo "Done with ${#FAILED[@]} FAILURE(S): ${FAILED[*]}"
    exit 1
fi