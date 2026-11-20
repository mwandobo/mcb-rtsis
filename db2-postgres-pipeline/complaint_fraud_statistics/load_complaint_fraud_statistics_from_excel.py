#!/usr/bin/env python3
"""Load complaintFraudStatistics from the Bancassurance business unit's Excel file.

Same situation as insurance-commission/insurance-claim/insurance-underwriting:
PROFITS has no bancassurance complaint/fraud data, so this report is sourced
entirely from the business's own spreadsheet, never DB2.

The "complaints and fraud statistics" sheet in the original Bancassurance
workbook (Bankassuarance (006).xlsx) was empty (all NIL). The only real
record available was recovered from a separate Excel crash-recovery file
(the business's working copy), supplied as a screenshot. That recovered
row's "policy Number" cell itself was corrupted (read back as the number
0) - per business direction, it is loaded as 'RN-2303' instead. The seed
file `complaint_fraud_statistics_seed.xlsx` in this folder reproduces that
one recovered row (with the corrected policy number) in the same sheet name
and column layout as the original workbook, so this loader also already
works unchanged against a future, fuller version of that workbook.

complaintNature/complaintStatus/complaintsReferred were checked directly
against Annexure 5 (D127, D107, D57) - the recovered row's values
("Repudiation of a claim" / "referred" / "Employer") are already exact,
valid lookup labels, so no mapping/correction was needed for this row.

Each run of this script REPLACES the entire contents of
"complaintFraudStatistics" with exactly what is in the given spreadsheet,
same as the other three bancassurance loaders.

Usage:
    python load_complaint_fraud_statistics_from_excel.py "<path to xlsx>"
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

SHEET_NAME = 'complaints and fraud statistics'

logger = logging.getLogger(__name__)


def _format_date(value):
    if pd.isna(value):
        return None
    if hasattr(value, 'strftime'):
        return pd.Timestamp(value).strftime('%d%m%Y%H%M')
    return pd.to_datetime(str(value).strip(), dayfirst=True).strftime('%d%m%Y%H%M')


def _text_or_unknown(value):
    if pd.isna(value):
        return 'Unknown'
    text = str(value).strip()
    return text or 'Unknown'


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
            CREATE TABLE IF NOT EXISTS "complaintFraudStatistics" (
                id BIGSERIAL PRIMARY KEY,
                "recordKey" VARCHAR(64) UNIQUE,
                "reportingDate" VARCHAR(12), "policyNumber" VARCHAR(255), "complaintNature" VARCHAR(255),
                "occurrenceDate" VARCHAR(12), "complaintReportingDate" VARCHAR(12), "closureDate" VARCHAR(12),
                currency VARCHAR(20), "tzsAmount" NUMERIC(31, 2), "complaintStatus" VARCHAR(50),
                "complaintsReferred" VARCHAR(100)
            )
        ''')
        cursor.execute('CREATE UNIQUE INDEX IF NOT EXISTS idx_complaintFraudStatistics_record_key ON "complaintFraudStatistics" ("recordKey")')
        connection.commit()


def load_rows(xlsx_path):
    df = pd.read_excel(xlsx_path, sheet_name=SHEET_NAME)
    df.columns = [str(c).strip() for c in df.columns]

    required = [
        'policy Number', 'complaint Nature', 'occurance Date', 'reporting Date',
        'closure Date', 'currency', 'TZS Amount', 'complaint Status', 'complaints Referred To',
    ]
    missing = [c for c in required if c not in df.columns]
    if missing:
        raise ValueError(f'Expected column(s) not found in "{SHEET_NAME}" sheet: {missing}')

    report_timestamp = pd.Timestamp.now().strftime('%d%m%Y%H%M')
    rows = []
    for _, r in df.iterrows():
        policy_number = _text_or_unknown(r['policy Number'])
        complaint_nature = _text_or_unknown(r['complaint Nature'])
        occurrence_date = _format_date(r['occurance Date'])
        complaint_reporting_date = _format_date(r['reporting Date'])
        closure_date = _format_date(r['closure Date'])
        currency = str(r['currency']).strip().upper()
        tzs_amount = float(r['TZS Amount'])
        complaint_status = _text_or_unknown(r['complaint Status'])
        complaints_referred = _text_or_unknown(r['complaints Referred To'])

        record_key = hashlib.sha256(
            '|'.join([policy_number, complaint_nature, occurrence_date or '']).encode('utf-8')
        ).hexdigest()

        rows.append((
            record_key, report_timestamp, policy_number, complaint_nature,
            occurrence_date, complaint_reporting_date, closure_date,
            currency, tzs_amount, complaint_status, complaints_referred,
        ))
    return rows


def replace_table(rows):
    setup_target_table()
    with postgres_connection() as connection, connection.cursor() as cursor:
        cursor.execute('DELETE FROM "complaintFraudStatistics"')
        psycopg2.extras.execute_values(cursor, '''
            INSERT INTO "complaintFraudStatistics" (
                "recordKey", "reportingDate", "policyNumber", "complaintNature",
                "occurrenceDate", "complaintReportingDate", "closureDate",
                currency, "tzsAmount", "complaintStatus", "complaintsReferred"
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
    logger.info('complaintFraudStatistics replaced from "%s": %s row(s) loaded.', sys.argv[1], len(loaded_rows))
