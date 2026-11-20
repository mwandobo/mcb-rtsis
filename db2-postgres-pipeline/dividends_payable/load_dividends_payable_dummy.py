#!/usr/bin/env python3
"""Seed dividendsPayable with dummy data.

Note: the BoT OpenAPI schema itself names this report "dividendsPaid"
(endpoint /equity/dividendsPaid, schema dividendsPaidInformation) - the
table/script here are named "dividendsPayable" per business preference,
matching the field names exactly either way (reportingDate, transactionDate,
dividendType, currency, orgAmount, tzsAmount, beneficiaryName,
beneficiaryCountry, beneficiaryAccNumber, beneficiaryBankCode).

Per explicit business direction: there is no real source (PROFITS has no
dividends-payable subsystem, and the business has no spreadsheet for it
either), so this report is populated with synthetic placeholder rows rather
than left empty, capped at 5 records - same approach as
core_capital_deductions/load_core_capital_deductions_dummy.py.

Every lookup-governed field is validated against a real BoT reference
(Annexure 5_Static and LookUp Values Version03 Sept 2025_ FINAL.xlsx) so the
submission itself is schema-valid, even though beneficiaries/amounts are
fictitious:
  - dividendType (D82 "Dividend Type", sheet "Attribute Table FINAL", rows
    ~1275-1279): only 3 real item values exist - 'Ordinary share capital',
    'Non Cumulative Non Redeemable Preference Shares', 'Cumulative
    Redeemable Preference Shares' - cycled across the 5 rows.
  - currency (sheet "Currencies"): 'TZS', a real item value.
  - beneficiaryCountry (sheet "Country codes"): 'TZ' (TANZANIA, UNITED
    REPUBLIC OF), a real ISO 3166-1 item value.
  - beneficiaryBankCode (sheet "FSP List", BIC column): 5 distinct real
    Tanzanian bank BIC codes (Absa, Azania, CRDB, DTB, Mwanga Hakika),
    reused in rotation.

beneficiaryName/beneficiaryAccNumber have no governing lookup, but both are
shaped to match this project's real data rather than an obviously-synthetic
label: beneficiaryName uses plausible Tanzanian names (not drawn from any
real customer record), and beneficiaryAccNumber follows the same 10-digit
numeric format seen in "accountInformation" (confirmed: always exactly 10
digits there), using a clearly-unused 0001000001-0001000005 block.

transactionDate is spread across the 5 months before reportingDate so it
never exceeds today, consistent with the project's "no future dates" rule
applied elsewhere.

Each run of this script REPLACES the entire contents of "dividendsPayable"
with exactly these 5 rows.

Usage:
    python load_dividends_payable_dummy.py
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

# (dividendType [D82], months_ago, orgAmount, beneficiaryName, beneficiaryAccNumber, beneficiaryBankCode [FSP List BIC])
DUMMY_ROWS = [
    ('Ordinary share capital', 1, 50000000.00, 'Juma Mwakalinga', '0001000001', 'BARCTZTZ'),
    ('Non Cumulative Non Redeemable Preference Shares', 2, 15000000.00, 'Neema Komba', '0001000002', 'AZANTZTZ'),
    ('Cumulative Redeemable Preference Shares', 3, 22000000.00, 'Baraka Mushi', '0001000003', 'CORUTZTZ'),
    ('Ordinary share capital', 4, 38000000.00, 'Zawadi Ngowi', '0001000004', 'DTKETZTZ'),
    ('Ordinary share capital', 5, 12000000.00, 'Elias Chambo', '0001000005', 'MWCBTZTZ'),
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
            CREATE TABLE IF NOT EXISTS "dividendsPayable" (
                id BIGSERIAL PRIMARY KEY,
                "recordKey" VARCHAR(64) UNIQUE,
                "reportingDate" VARCHAR(12), "transactionDate" VARCHAR(12),
                "dividendType" VARCHAR(255), currency VARCHAR(20),
                "orgAmount" NUMERIC(31, 2), "tzsAmount" NUMERIC(31, 2),
                "beneficiaryName" VARCHAR(255), "beneficiaryCountry" VARCHAR(10),
                "beneficiaryAccNumber" VARCHAR(50), "beneficiaryBankCode" VARCHAR(20)
            )
        ''')
        cursor.execute('CREATE UNIQUE INDEX IF NOT EXISTS idx_dividendsPayable_record_key ON "dividendsPayable" ("recordKey")')
        connection.commit()


def build_rows():
    now = pd.Timestamp.now()
    report_timestamp = now.strftime('%d%m%Y%H%M')
    rows = []
    for dividend_type, months_ago, org_amount, beneficiary_name, acc_number, bank_code in DUMMY_ROWS:
        transaction_date = (now - pd.DateOffset(months=months_ago)).strftime('%d%m%Y%H%M')
        currency = 'TZS'
        tzs_amount = org_amount  # already TZS, no FX conversion needed
        beneficiary_country = 'TZ'

        record_key = hashlib.sha256(
            '|'.join([acc_number, dividend_type, transaction_date]).encode('utf-8')
        ).hexdigest()

        rows.append((
            record_key, report_timestamp, transaction_date,
            dividend_type, currency, org_amount, tzs_amount,
            beneficiary_name, beneficiary_country, acc_number, bank_code,
        ))
    return rows


def replace_table(rows):
    setup_target_table()
    with postgres_connection() as connection, connection.cursor() as cursor:
        cursor.execute('DELETE FROM "dividendsPayable"')
        psycopg2.extras.execute_values(cursor, '''
            INSERT INTO "dividendsPayable" (
                "recordKey", "reportingDate", "transactionDate",
                "dividendType", currency, "orgAmount", "tzsAmount",
                "beneficiaryName", "beneficiaryCountry", "beneficiaryAccNumber", "beneficiaryBankCode"
            ) VALUES %s
        ''', rows)
        connection.commit()


if __name__ == '__main__':
    logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
    seeded_rows = build_rows()
    replace_table(seeded_rows)
    logger.info('dividendsPayable replaced with %s dummy row(s).', len(seeded_rows))
