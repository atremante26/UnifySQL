#!/bin/bash
CONTAINER="docker-postgres-1"
PG_HOST="localhost"
PG_PORT="5433"
PG_USER="postgres"
PG_PASS="postgres"
BASE_DIR="benchmarks/spider/database"

# Databases skipped on purpose — every entry needs a documented reason.
EXCLUDED_DBS=(
    "car_1"   # pgloader types join columns inconsistently (bigint vs text), breaking gold SQL joins
)

DB_IDS=()
for dir in "$BASE_DIR"/*/; do
    db_id="$(basename "$dir")"
    skip=""
    for excluded in "${EXCLUDED_DBS[@]}"; do
        if [ "$db_id" = "$excluded" ]; then
            skip=1
            break
        fi
    done
    [ -n "$skip" ] && continue
    DB_IDS+=("$db_id")
done

FAILED=()
STAGE_DIR="/tmp/spider_load"
mkdir -p "$STAGE_DIR"
SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"

echo "Loading ${#DB_IDS[@]} Spider databases via pgloader (${#EXCLUDED_DBS[@]} excluded)..."

for db_id in "${DB_IDS[@]}"; do
    echo ""
    echo "  Processing $db_id..."

    sqlite_file="$BASE_DIR/$db_id/$db_id.sqlite"

    if [ ! -f "$sqlite_file" ]; then
        echo "  FAIL $db_id — sqlite file not found at $sqlite_file"
        FAILED+=("$db_id")
        continue
    fi

    pg_db="spider_$(echo $db_id | tr '[:upper:]' '[:lower:]')"

    docker exec $CONTAINER psql -U $PG_USER \
        -c "SELECT pg_terminate_backend(pid) FROM pg_stat_activity WHERE datname = '$pg_db' AND pid <> pg_backend_pid();" > /dev/null 2>&1

    if ! docker exec $CONTAINER psql -U $PG_USER \
        -c "DROP DATABASE IF EXISTS $pg_db;" > /dev/null 2>&1; then
        echo "  FAIL $db_id — could not drop $pg_db"
        FAILED+=("$db_id")
        continue
    fi
    docker exec $CONTAINER psql -U $PG_USER \
        -c "CREATE DATABASE $pg_db;" > /dev/null 2>&1

    staged="$STAGE_DIR/$db_id.sqlite"
    cp "$sqlite_file" "$staged"
    if ! python3 "$SCRIPT_DIR/sanitize_sqlite.py" "$staged"; then
        echo "  FAIL $db_id — sanitize step failed"
        FAILED+=("$db_id")
        continue
    fi

    # Spider schema bugs: columns declared int whose data is real. float-to-string
    # makes pgloader format the values as decimals instead of Lisp literals (1.5d0).
    extra_casts=""
    case "$db_id" in
        bike_1)                extra_casts='column weather.precipitation_inches to real using float-to-string,' ;;
        soccer_1)              extra_casts='column Player.height to real using float-to-string,' ;;
        university_basketball) extra_casts='column basketball_match.All_Games_Percent to real using float-to-string,' ;;
    esac

    # Type casts: bool columns hold free text in Spider (keep as text), and
    # MySQL-style declared types (int(11), tinyint, year, ...) need explicit
    # normalization — pgloader passes them through to Postgres DDL otherwise.
    load_file="/tmp/pgloader_${pg_db}.load"
    cat > "$load_file" <<EOF
LOAD DATABASE
    FROM sqlite://$staged
    INTO postgresql://$PG_USER:$PG_PASS@$PG_HOST:$PG_PORT/$pg_db

    CAST $extra_casts
         type bool to text,
         type boolean to text,
         type int to integer drop typemod,
         type bigint to bigint drop typemod,
         type smallint to smallint drop typemod,
         type tinyint to smallint drop typemod,
         type mediumint to integer drop typemod,
         type char to text drop typemod,
         type year to integer drop typemod

    WITH include drop, create tables, create indexes, reset sequences
;
EOF

    pgloader_output=$(pgloader "$load_file" 2>&1)
    pgloader_status=$?
    rm -f "$load_file"

    if [ $pgloader_status -ne 0 ]; then
        echo "  FAIL $db_id — pgloader error:"
        echo "$pgloader_output" | tail -5 | sed 's/^/    /'
        FAILED+=("$db_id")
        continue
    fi

    # pgloader exits 0 even when rows are rejected, so verify source vs target:
    # its report summary lists each data table (between the 2nd and 3rd dashed
    # separators) with error and imported-row counts; compare imported rows
    # against the staged sqlite's row count. Empty source tables pass.
    summary=$(echo "$pgloader_output" | awk '
        /^-+  / { n++; next }
        n == 2 { sub(/^ +/, ""); nf = split($0, f, /  +/); if (nf >= 3) print f[1] "\t" f[2] "\t" f[3] }')

    load_errors=""
    while IFS= read -r tbl; do
        src_count=$(sqlite3 "$staged" "SELECT count(*) FROM \"$tbl\"")
        row=$(echo "$summary" | awk -F'\t' -v t="$tbl" 'tolower($1) == tolower(t) { print; exit }')
        if [ -z "$row" ]; then
            if [ "$src_count" -ne 0 ]; then
                load_errors+="    $tbl: missing from pgloader summary (source has $src_count rows)
"
            fi
            continue
        fi
        tbl_errors=$(echo "$row" | cut -f2)
        tbl_rows=$(echo "$row" | cut -f3)
        if [ "$tbl_errors" -ne 0 ]; then
            load_errors+="    $tbl: $tbl_errors error(s), loaded $tbl_rows of $src_count rows
"
        elif [ "$tbl_rows" -lt "$src_count" ]; then
            load_errors+="    $tbl: loaded $tbl_rows of $src_count rows
"
        fi
    done < <(sqlite3 "$staged" "SELECT name FROM sqlite_master WHERE type='table' AND name NOT LIKE 'sqlite_%'")

    if [ -n "$load_errors" ]; then
        echo "  FAIL $db_id — source rows lost in transfer:"
        printf '%s' "$load_errors"
        echo "$pgloader_output" | grep -i error | head -5 | sed 's/^/    /'
        FAILED+=("$db_id")
        continue
    fi

    rm -f "$staged"
    echo "  SUCCESS $db_id loaded: $pg_db"
done

echo ""
if [ ${#FAILED[@]} -eq 0 ]; then
    echo "Done — all ${#DB_IDS[@]} databases loaded in ${SECONDS}s."
else
    echo "Done in ${SECONDS}s with ${#FAILED[@]} FAILURE(S): ${FAILED[*]}"
    exit 1
fi
