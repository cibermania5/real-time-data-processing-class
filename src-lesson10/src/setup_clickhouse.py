"""Create the Lesson 10 ClickHouse tables and materialized views.

Usage:
    uv run python src/setup_clickhouse.py
    uv run python src/setup_clickhouse.py --reset   # drop and recreate
"""

import argparse
import sys
from pathlib import Path

from config import wait_for_clickhouse

# Find the SQL file relative to this script.
SQL_DIR = Path(__file__).parent.parent / "sql"
INIT_SQL = SQL_DIR / "init_clickhouse.sql"


def remove_comments(sql: str) -> str:
    """Strip SQL line comments and block comments so semicolons inside them
    do not break statement splitting."""
    import re

    sql = re.sub(r"/\*.*?\*/", "", sql, flags=re.DOTALL)
    lines = [line for line in sql.splitlines() if not line.strip().startswith("--")]
    return "\n".join(lines)


def split_statements(sql: str):
    """Split SQL on semicolons, ignoring empty blocks and comments."""
    clean = remove_comments(sql)
    statements = []
    for raw in clean.split(";"):
        cleaned = raw.strip()
        if cleaned:
            statements.append(cleaned)
    return statements


def main():
    parser = argparse.ArgumentParser(description="create L10 ClickHouse schema")
    parser.add_argument("--reset", action="store_true", help="drop existing tables first")
    args = parser.parse_args()

    print("waiting for ClickHouse...")
    client = wait_for_clickhouse()

    if args.reset:
        drops = [
            "DROP VIEW IF EXISTS revenue_by_region_minute_mv",
            "DROP TABLE IF EXISTS revenue_by_region_minute",
            "DROP VIEW IF EXISTS unique_users_by_region_minute_mv",
            "DROP TABLE IF EXISTS unique_users_by_region_minute",
            "DROP VIEW IF EXISTS events_kafka_mv",
            "DROP TABLE IF EXISTS events_kafka",
            "DROP TABLE IF EXISTS events",
        ]
        for stmt in drops:
            print(f"  running: {stmt}")
            try:
                client.command(stmt)
            except Exception as exc:  # noqa: BLE001
                print(f"  warning: {exc}", file=sys.stderr)

    sql = INIT_SQL.read_text()
    for stmt in split_statements(sql):
        print(f"  running: {stmt.splitlines()[0]}")
        client.command(stmt)

    print("\nClickHouse schema is ready.")


if __name__ == "__main__":
    main()
