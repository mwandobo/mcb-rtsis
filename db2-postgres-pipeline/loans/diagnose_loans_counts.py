#!/usr/bin/env python3
"""
Diagnose where loan rows are reduced between LOAN_ACCOUNT and the final v9 loans query.
"""

import logging
import os
import sys
from pathlib import Path

import psycopg2

# Add parent directory to path for imports
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from config import Config
from db2_connection import DB2Connection


logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")


def build_queries(sql_text: str) -> dict[str, str]:
    """Build count queries for key reduction points in the final loans SQL."""
    sql = sql_text.replace("{timestamp_filter}", "").strip().rstrip(";")

    marker = "\nSELECT *\nFROM (\n"
    if marker not in sql:
        raise ValueError("Could not locate final SELECT block in loan-information-v9.sql")

    cte_part, _ = sql.split(marker, 1)
    cte_part = cte_part.rstrip()

    dedupe_subquery = """
SELECT mq.*,
       ROW_NUMBER() OVER (
           PARTITION BY mq.loanNumber
           ORDER BY mq.customerIdentificationNumber, mq.accountNumber
       ) AS rn
FROM MainQuery mq
""".strip()

    officer_filter = """
LENGTH(TRIM(TRANSLATE(t.loanOfficer, '', '0123456789'))) = LENGTH(TRIM(t.loanOfficer))
AND LENGTH(TRIM(TRANSLATE(t.loanSupervisor, '', '0123456789'))) = LENGTH(TRIM(t.loanSupervisor))
""".strip()

    queries = {
        "raw_loan_account": "SELECT COUNT(*) AS count FROM LOAN_ACCOUNT",
        "main_query_rows": f"{cte_part}\nSELECT COUNT(*) AS count FROM MainQuery",
        "main_query_distinct_loan_numbers": f"{cte_part}\nSELECT COUNT(DISTINCT loanNumber) AS count FROM MainQuery",
        "deduped_rows_before_officer_filters": (
            f"{cte_part}\nSELECT COUNT(*) AS count\n"
            f"FROM (\n{dedupe_subquery}\n) t\n"
            "WHERE t.rn = 1"
        ),
        "rows_removed_by_officer_filters": (
            f"{cte_part}\nSELECT COUNT(*) AS count\n"
            f"FROM (\n{dedupe_subquery}\n) t\n"
            "WHERE t.rn = 1\n"
            f"  AND NOT ({officer_filter})"
        ),
        "final_query_rows": (
            f"{cte_part}\nSELECT COUNT(*) AS count\n"
            f"FROM (\n{dedupe_subquery}\n) t\n"
            "WHERE t.rn = 1\n"
            f"  AND {officer_filter}"
        ),
    }
    return queries


def get_postgres_count(config: Config) -> int | None:
    """Return current PostgreSQL row count for loanInformation if reachable."""
    try:
        conn = psycopg2.connect(
            host=config.database.pg_host,
            port=config.database.pg_port,
            database=config.database.pg_database,
            user=config.database.pg_user,
            password=config.database.pg_password,
        )
        cur = conn.cursor()
        cur.execute('SELECT COUNT(*) FROM "loanInformation"')
        count = cur.fetchone()[0]
        cur.close()
        conn.close()
        return count
    except Exception as exc:
        logging.warning(f"Could not fetch PostgreSQL loanInformation count: {exc}")
        return None


def main() -> None:
    repo_root = Path(__file__).resolve().parents[2]
    sql_path = repo_root / "sqls" / "loan-information-v9.sql"
    sql_text = sql_path.read_text(encoding="utf-8")

    queries = build_queries(sql_text)
    config = Config()
    db2 = DB2Connection()

    logging.info(f"Using SQL from: {sql_path}")
    logging.info("Running DB2 diagnostics...")

    results: list[tuple[str, int]] = []
    with db2.get_connection(log_connection=True) as conn:
        cur = conn.cursor()
        for name, query in queries.items():
            cur.execute(query)
            count = int(cur.fetchone()[0])
            results.append((name, count))

    pg_count = get_postgres_count(config)

    print("\nLoans Count Diagnostics")
    print("-" * 40)
    for name, count in results:
        print(f"{name}: {count:,}")
    if pg_count is not None:
        print(f"postgres_loaded_rows: {pg_count:,}")


if __name__ == "__main__":
    main()
