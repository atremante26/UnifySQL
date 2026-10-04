import re
import sqlite3
import sys

DATE_KEYWORDS = ("DATE", "TIME")
NUMERIC_KEYWORDS = ("INT", "REAL", "FLOAT", "DOUBLE", "NUMERIC", "DECIMAL", "NUMBER", "YEAR")

TYPE_REWRITES = (
    (re.compile(r"\bvarchar2\b", re.IGNORECASE), "varchar"),
    (re.compile(r"\bnumber\s*\([^)]*\)", re.IGNORECASE), "numeric"),
    (re.compile(r"\bcharacter\s+varchar\b", re.IGNORECASE), "varchar"),
    (re.compile(r"\b(tinyint|smallint|mediumint|integer|int|bigint)\s+unsigned\b", re.IGNORECASE), r"\1"),
)


def is_number(val):
    try:
        float(val)
        return True
    except (TypeError, ValueError):
        return False


def normalize_declared_types(path):
    """Rewrite declared column types pgloader's sqlite type grammar rejects."""
    con = sqlite3.connect(path)
    con.text_factory = lambda b: b.decode("utf-8", "replace")
    cur = con.cursor()
    rows = cur.execute(
        "SELECT name, sql FROM sqlite_master WHERE type='table' AND name NOT LIKE 'sqlite_%'"
    ).fetchall()
    changes = []
    for name, sql in rows:
        new_sql = sql
        for pattern, replacement in TYPE_REWRITES:
            new_sql = pattern.sub(replacement, new_sql)
        if new_sql != sql:
            changes.append((name, new_sql))
    if changes:
        cur.execute("PRAGMA writable_schema=1")
        for name, new_sql in changes:
            cur.execute(
                "UPDATE sqlite_master SET sql=? WHERE type='table' AND name=?",
                (new_sql, name),
            )
        cur.execute("PRAGMA writable_schema=0")
        con.commit()
    con.close()


def sanitize_data(path):
    con = sqlite3.connect(path)
    con.text_factory = bytes
    cur = con.cursor()

    tables = [
        r[0].decode() if isinstance(r[0], bytes) else r[0]
        for r in cur.execute(
            "SELECT name FROM sqlite_master WHERE type='table' AND name NOT LIKE 'sqlite_%'"
        )
    ]

    for table in tables:
        cols = cur.execute(f'PRAGMA table_info("{table}")').fetchall()
        date_cols = []
        numeric_cols = []
        text_cols = []
        for _, name, ctype, *_ in cols:
            name = name.decode() if isinstance(name, bytes) else name
            ctype = (ctype.decode() if isinstance(ctype, bytes) else (ctype or "")).upper()
            if any(k in ctype for k in DATE_KEYWORDS):
                date_cols.append(name)
            elif any(k in ctype for k in NUMERIC_KEYWORDS):
                numeric_cols.append(name)
            else:
                text_cols.append(name)

        for col in date_cols:
            try:
                cur.execute(
                    f'UPDATE "{table}" SET "{col}"=NULL '
                    f"WHERE \"{col}\"='' OR \"{col}\" GLOB '0000-00-00*'"
                )
            except sqlite3.IntegrityError:
                # NOT NULL column: the zero date is unrepresentable, drop the row
                # (e.g. hr_1's dummy job_history row). Verification counts rows
                # against the staged copy, so this stays consistent.
                cur.execute(
                    f'DELETE FROM "{table}" '
                    f"WHERE \"{col}\"='' OR \"{col}\" GLOB '0000-00-00*'"
                )

        for col in numeric_cols:
            bad_rowids = [
                rowid
                for rowid, val in cur.execute(
                    f'SELECT rowid, "{col}" FROM "{table}" WHERE typeof("{col}")=\'text\''
                ).fetchall()
                if not is_number(val)
            ]
            for rowid in bad_rowids:
                cur.execute(f'UPDATE "{table}" SET "{col}"=NULL WHERE rowid=?', (rowid,))

        if not text_cols:
            continue
        col_list = ", ".join(f'"{c}"' for c in text_cols)
        for row in cur.execute(f'SELECT rowid, {col_list} FROM "{table}"').fetchall():
            rowid, values = row[0], row[1:]
            fixes = {}
            for col, val in zip(text_cols, values):
                if isinstance(val, bytes):
                    try:
                        val.decode("utf-8")
                    except UnicodeDecodeError:
                        fixes[col] = val.decode("utf-8", "replace")
            if fixes:
                set_clause = ", ".join(f'"{c}"=?' for c in fixes)
                cur.execute(
                    f'UPDATE "{table}" SET {set_clause} WHERE rowid=?',
                    (*fixes.values(), rowid),
                )

    con.commit()
    con.close()


if __name__ == "__main__":
    normalize_declared_types(sys.argv[1])
    sanitize_data(sys.argv[1])
