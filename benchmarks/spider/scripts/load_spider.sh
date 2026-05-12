# Loads all 14 golden set Spider databases into Docker Postgres using pgloader.
#
# Prerequisites:
#   - Docker stack running: docker-compose -f docker/docker-compose.yaml up -d
#   - pgloader installed: brew install pgloader
#
# Usage:
#   ./benchmarks/spider/scripts/load_spider_pgloader.sh

CONTAINER="docker-postgres-1"
PG_HOST="localhost"
PG_PORT="5432"
PG_USER="postgres"
PG_PASS="postgres"
BASE_DIR="benchmarks/spider/database"

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

echo "Loading ${#DB_IDS[@]} Spider databases via pgloader..."

for db_id in "${DB_IDS[@]}"; do
    echo ""
    echo "  Processing $db_id..."

    sqlite_file="$BASE_DIR/$db_id/$db_id.sqlite"

    if [ ! -f "$sqlite_file" ]; then
        echo "  FAIL $db_id — sqlite file not found at $sqlite_file"
        continue
    fi

    # Use lowercase for Postgres database name to avoid case sensitivity issues
    pg_db="spider_$(echo $db_id | tr '[:upper:]' '[:lower:]')"

    # Drop and recreate database
    docker exec $CONTAINER psql -U $PG_USER \
        -c "DROP DATABASE IF EXISTS $pg_db;" > /dev/null 2>&1
    docker exec $CONTAINER psql -U $PG_USER \
        -c "CREATE DATABASE $pg_db;" > /dev/null 2>&1

    # Load via pgloader — FK errors are warnings, not failures
    pgloader --quiet "$sqlite_file" \
        "postgresql://$PG_USER:$PG_PASS@$PG_HOST:$PG_PORT/$pg_db" > /dev/null 2>&1

    if [ $? -eq 0 ]; then
        echo "  SUCCESS $db_id loaded: $pg_db"
    else
        echo "  WARNING $db_id loaded with warnings: $pg_db (FK constraints may have failed)"
    fi
done

echo ""
echo "Done."