#!/usr/bin/env python3
"""Seed coreCapitalDeductions with dummy data.

Per explicit business direction: there is no real source (PROFITS has no
capital-deductions subsystem, and the business has no spreadsheet for it
either), so this report is populated with synthetic placeholder rows rather
than left empty, capped at 5 records.

Only deductionsType and currency are validated against real BoT lookups
(Annexure 5_Static and LookUp Values Version03 Sept 2025_ FINAL.xlsx) so the
submission itself is schema-valid, even though the underlying transactions
are fictitious:
  - deductionsType (D149 "Capital deduction", sheet "Attribute Table FINAL",
    rows ~2021-2029): 5 of the 7 real item values are used, one per row -
    'Year to date losses', 'Pre-operating expenses', 'Prepaid expenses',
    'Leasehold rights', 'Goodwill'.
  - currency (sheet "Currencies"): 'TZS', a real item value.

transactionDate is spread across the 5 months before reportingDate so it
never exceeds today, consistent with the project's "no future dates" rule
applied elsewhere (interbank-loan-payable disbursementDate,
insurance-claim incidentOccurrenceDate).

Each run of this script REPLACES the entire contents of
"coreCapitalDeductions" with exactly these 5 rows.

Usage:
    python load_core_capital_deductions_dummy.py
"""

import hashlib
import logging
import os
import sys
from contextlib import contextmanager

import pandas as pd
import psycopg2
import psycopg2.extras

sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from config import Config

logger = logging.getLogger(__name__)

# D149 "Capital deduction" item values (5 of 7 real entries, one per dummy
# row) paired with a plausible dummy org/tzs amount and a transactionDate
# offset (months before today) so dates stay in the past.
DUMMY_ROWS = [
    ('Year to date losses', 1, 120000000.00),
    ('Pre-operating expenses', 2, 45000000.00),
    ('Prepaid expenses', 3, 18500000.00),
    ('Leasehold rights', 4, 9000000.00),
    ('Goodwill', 5, 60000000.00),
]


@contextmanager
def postgres_connection():
    cfg = Config()
    connection = psycopg2.connect(
        host=cfg.database.pg_host, port=cfg.database.pg_port,
        database=cfg.database.pg_database, user=cfg.database.pg_user,
        password=cfg.database.pg_password,
    )
    try:
        yield connection
    finally:
        connection.close()


def setup_target_table():
    with postgres_connection() as connection, connection.cursor() as cursor:
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS "coreCapitalDeductions" (
                id BIGSERIAL PRIMARY KEY,
                "recordKey" VARCHAR(64) UNIQUE,
                "reportingDate" VARCHAR(12), "transactionDate" VARCHAR(12),
                "deductionsType" VARCHAR(255), currency VARCHAR(20),
                "orgAmount" NUMERIC(31, 2), "tzsAmount" NUMERIC(31, 2)
            )
        ''')
        cursor.execute('CREATE UNIQUE INDEX IF NOT EXISTS idx_coreCapitalDeductions_record_key ON "coreCapitalDeductions" ("recordKey")')
        connection.commit()


def build_rows():
    now = pd.Timestamp.now()
    report_timestamp = now.strftime('%d%m%Y%H%M')
    rows = []
    for deductions_type, months_ago, org_amount in DUMMY_ROWS:
        transaction_date = (now - pd.DateOffset(months=months_ago)).strftime('%d%m%Y%H%M')
        currency = 'TZS'
        tzs_amount = org_amount  # already TZS, no FX conversion needed

        record_key = hashlib.sha256(
            '|'.join([deductions_type, transaction_date]).encode('utf-8')
        ).hexdigest()

        rows.append((
            record_key, report_timestamp, transaction_date,
            deductions_type, currency, org_amount, tzs_amount,
        ))
    return rows


def replace_table(rows):
    setup_target_table()
    with postgres_connection() as connection, connection.cursor() as cursor:
        cursor.execute('DELETE FROM "coreCapitalDeductions"')
        psycopg2.extras.execute_values(cursor, '''
            INSERT INTO "coreCapitalDeductions" (
                "recordKey", "reportingDate", "transactionDate",
                "deductionsType", currency, "orgAmount", "tzsAmount"
            ) VALUES %s
        ''', rows)
        connection.commit()


if __name__ == '__main__':
    logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
    seeded_rows = build_rows()
    replace_table(seeded_rows)
    logger.info('coreCapitalDeductions replaced with %s dummy row(s).', len(seeded_rows))
