#!/usr/bin/env python3
"""Load insuranceCommission from the Bancassurance business unit's Excel file.

The GL-posting-based source (sqls/insurance-commission.sql, GL 503010011/
503010012) was confirmed misleading: GLI_TRX_EXTRACT has no insurance-policy
master data, so policyNumber fell back to the ledger's own narrative text
(often unrelated noise like "EXCISE DUTY AND VAT") and commissionRate had no
source at all and was always 0.

The Bancassurance unit's own spreadsheet ("Insuarance Commision" sheet) is
the authoritative source instead: real policyNumber, real commissionRate,
real amount and date per policy, confirmed by the business as true data.

Each run of this script REPLACES the entire contents of "insuranceCommission"
with exactly what is in the given spreadsheet - the table always reflects
the latest Excel snapshot, not an accumulation of every file ever loaded.

Usage:
    python load_insurance_commission_from_excel.py "<path to xlsx>"
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

SHEET_NAME = 'Insuarance Commision'

logger = logging.getLogger(__name__)


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
            CREATE TABLE IF NOT EXISTS "insuranceCommission" (
                id BIGSERIAL PRIMARY KEY,
                "recordKey" VARCHAR(64) UNIQUE,
                "reportingDate" VARCHAR(12), "policyNumber" VARCHAR(255), currency VARCHAR(20),
                "orgCommissionReceivedAmount" NUMERIC(31, 2), "tzsCommissionReceivedAmount" NUMERIC(31, 2),
                "commissionReceivedDate" VARCHAR(12), "commissionRate" NUMERIC(31, 6)
            )
        ''')
        cursor.execute('CREATE UNIQUE INDEX IF NOT EXISTS idx_insuranceCommission_record_key ON "insuranceCommission" ("recordKey")')
        connection.commit()


def load_rows(xlsx_path):
    df = pd.read_excel(xlsx_path, sheet_name=SHEET_NAME)
    df.columns = [str(c).strip() for c in df.columns]

    required = [
        'policy Number', 'currency', 'original Amount Commision Received',
        'date Commission Received', 'commission Rate (%)',
    ]
    missing = [c for c in required if c not in df.columns]
    if missing:
        raise ValueError(f'Expected column(s) not found in "{SHEET_NAME}" sheet: {missing}')

    report_timestamp = pd.Timestamp.now().strftime('%d%m%Y%H%M')
    rows = []
    for _, r in df.iterrows():
        policy_number = str(r['policy Number']).strip()
        currency = str(r['currency']).strip().upper()
        org_amount = float(r['original Amount Commision Received'])
        commission_date = pd.Timestamp(r['date Commission Received']).strftime('%d%m%Y%H%M')
        commission_rate = float(r['commission Rate (%)'])

        if currency != 'TZS':
            # Every row in every file loaded so far has been TZS. This is a
            # real, unresolved gap rather than a guess: raising here (instead
            # of silently defaulting tzsAmount = orgAmount) means a future
            # foreign-currency row gets noticed and the FX conversion added
            # deliberately, not papered over.
            raise ValueError(
                f'Row for policy {policy_number} is in {currency}, not TZS - '
                'FX conversion to tzsCommissionReceivedAmount is not implemented yet.'
            )
        tzs_amount = org_amount

        record_key = hashlib.sha256(policy_number.encode('utf-8')).hexdigest()
        rows.append((
            record_key, report_timestamp, policy_number, currency,
            org_amount, tzs_amount, commission_date, commission_rate,
        ))
    return rows


def replace_table(rows):
    setup_target_table()
    with postgres_connection() as connection, connection.cursor() as cursor:
        cursor.execute('DELETE FROM "insuranceCommission"')
        psycopg2.extras.execute_values(cursor, '''
            INSERT INTO "insuranceCommission" (
                "recordKey", "reportingDate", "policyNumber", currency, "orgCommissionReceivedAmount",
                "tzsCommissionReceivedAmount", "commissionReceivedDate", "commissionRate"
            ) VALUES %s
        ''', rows)
        connection.commit()


if __name__ == '__main__':
    logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
    if len(sys.argv) != 2:
        print(f'Usage: python {os.path.basename(__file__)} "<path to xlsx>"')
        sys.exit(1)
    loaded_rows = load_rows(sys.argv[1])
    replace_table(loaded_rows)
    logger.info('insuranceCommission replaced from "%s": %s row(s) loaded.', sys.argv[1], len(loaded_rows))
