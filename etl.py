#!/usr/bin/env python3
import argparse, csv, sqlite3
from pathlib import Path

def ensure_db_conn(db_path: str) -> sqlite3.Connection:
    conn = sqlite3.connect(db_path)
    conn.execute("PRAGMA foreign_keys = ON;")
    return conn

def run_schema(conn, schema_file: Path):
    sql = schema_file.read_text(encoding="utf-8")
    with conn:
        conn.executescript(sql)

def _coerce_cell(col: str, val: str):
    if val is None or val == "":
        return None
    col_low = col.lower()
    # einfache Heuristik für Demo-Schema (customers)
    if col_low in {"id"}:
        return int(val)
    if col_low in {"spend"}:
        return float(val)
    # signup_date bleibt TEXT (ISO-String)
    return val

def import_csv(conn, table: str, csv_file: Path, batch_size: int = 1000) -> int:
    with csv_file.open("r", encoding="utf-8", newline="") as f:
        reader = csv.DictReader(f)
        headers = [h.strip() for h in (reader.fieldnames or [])]
        if not headers:
            raise ValueError("CSV hat keine Header-Zeile.")

        placeholders = ", ".join(["?"] * len(headers))
        cols = ", ".join([f'"{c}"' for c in headers])
        sql = f'INSERT INTO "{table}" ({cols}) VALUES ({placeholders})'

        total = 0
        batch = []
        for row in reader:
            coerced = [_coerce_cell(h, row.get(h)) for h in headers]
            batch.append(coerced)
            if len(batch) >= batch_size:
                with conn:
                    conn.executemany(sql, batch)
                total += len(batch)
                batch.clear()

        if batch:
            with conn:
                conn.executemany(sql, batch)
            total += len(batch)

        print(f"[OK] {total} Zeilen importiert in '{table}'")
        return total

def run_query(conn, q: str):
    cur = conn.execute(q)
    rows = cur.fetchall()
    if cur.description:
        headers = [d[0] for d in cur.description]
        print(" | ".join(headers))
        print("-" * (len(" | ".join(headers))))
    for r in rows:
        print(" | ".join("" if v is None else str(v) for v in r))

def main():
    p = argparse.ArgumentParser(description="CSV → SQLite ETL (Mini-Projekt)")
    p.add_argument("--db", required=True, help="Pfad zur SQLite-DB (z.B. demo.sqlite)")
    p.add_argument("--schema", help="Schema-Datei (z.B. schema.sql)")
    p.add_argument("--table", help="Zieltabelle (z.B. customers)")
    p.add_argument("--csv", help="CSV-Datei (z.B. data/customers.csv)")
    p.add_argument("--query", help="SQL-Query")
    a = p.parse_args()

    conn = ensure_db_conn(a.db)
    try:
        if a.schema:
            run_schema(conn, Path(a.schema))
            print(f"[OK] Schema angewendet: {a.schema}")
        if a.table and a.csv:
            import_csv(conn, a.table, Path(a.csv))
        if a.query:
            run_query(conn, a.query)
        if not any([a.schema, a.csv, a.query]):
            print("Nichts zu tun. Verwende --schema / --csv + --table / --query.")
    finally:
        conn.close()

if __name__ == "__main__":
    main()
