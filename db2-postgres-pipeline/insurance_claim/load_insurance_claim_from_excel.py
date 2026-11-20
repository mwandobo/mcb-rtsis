#!/usr/bin/env python3
"""Load insuranceClaim from the Bancassurance business unit's Excel file.

PROFITS has no insurance-claims subsystem (same gap confirmed for
insurance-commission - see load_insurance_commission_from_excel.py), so this
report is sourced entirely from the business's own spreadsheet
("insuarance claim" sheet), never from DB2.

The sheet is missing several BoT-mandatory fields entirely: insurerStatus,
orgPaidClaimAmount/tzsPaidClaimAmount, claimStatus, repudiationReason,
claimClosureDate. Per business direction these are populated with the most
conservative, schema-valid placeholder rather than left blank or guessed at
per-row:
    insurerStatus    = 'active'           (D64: status of operation)
    claimStatus      = 'Open'             (D80: status of claims)
    orgPaidClaimAmount / tzsPaidClaimAmount = 0   (consistent with 'Open' -
                                                    nothing paid out yet)
    repudiationReason = 'Not Applicable'  (claim was not repudiated)
    claimClosureDate  = reportingDate     (no real closure date; same
                                            never-in-the-past convention used
                                            elsewhere in this project)
The sheet's own "status Of Insurer" column is NOT used for insurerStatus -
its values ("General Insurance" / "Life Insurance") are the insurance type,
not an active/inactive status, so mapping it in would misrepresent the data.

One row has a blank policyNumber (mandatory) in the source; per business
direction it is kept and set to 'Unknown' rather than dropped or fabricated.
claimRefNumber is blank on 2 of 3 rows for the same reason - also 'Unknown'.

Each run of this script REPLACES the entire contents of "insuranceClaim"
with exactly what is in the given spreadsheet, same as the commission loader.

Usage:
    python load_insurance_claim_from_excel.py "<path to xlsx>"
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

SHEET_NAME = 'insuarance claim'

logger = logging.getLogger(__name__)


def _parse_date(value):
    """The sheet mixes real datetime cells with plain dd/mm/yyyy strings."""
    if pd.isna(value):
        return None
    if hasattr(value, 'strftime'):
        return pd.Timestamp(value).strftime('%d%m%Y%H%M')
    return pd.to_datetime(str(value).strip(), dayfirst=True).strftime('%d%m%Y%H%M')


def _text_or_unknown(value):
    if pd.isna(value):
        return 'Unknown'
    text = str(value).strip()
    if text.endswith('.0'):  # e.g. claim reference numbers read in as floats
        text = text[:-2]
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
            CREATE TABLE IF NOT EXISTS "insuranceClaim" (
                id BIGSERIAL PRIMARY KEY,
                "recordKey" VARCHAR(64) UNIQUE,
                "reportingDate" VARCHAR(12), "policyNumber" VARCHAR(255), "insurerStatus" VARCHAR(50),
                "claimRefNumber" VARCHAR(255), "claimDate" VARCHAR(12), "claimNature" VARCHAR(500),
                "incidentOccurrenceDate" VARCHAR(12), "incidentReportingDate" VARCHAR(12), currency VARCHAR(20),
                "orgSumClaimAmount" NUMERIC(31, 2), "tzsSumClaimAmount" NUMERIC(31, 2),
                "orgPaidClaimAmount" NUMERIC(31, 2), "tzsPaidClaimAmount" NUMERIC(31, 2),
                "claimStatus" VARCHAR(20), "repudiationReason" VARCHAR(500), "claimClosureDate" VARCHAR(12)
            )
        ''')
        cursor.execute('CREATE UNIQUE INDEX IF NOT EXISTS idx_insuranceClaim_record_key ON "insuranceClaim" ("recordKey")')
        connection.commit()


def load_rows(xlsx_path):
    df = pd.read_excel(xlsx_path, sheet_name=SHEET_NAME)
    df.columns = [str(c).strip() for c in df.columns]

    required = [
        'policy Number', 'status Of Insurer', 'claim Reference Number', 'claim Date',
        'nature of Claim', 'incident Occurrence Date', 'incident Reporting Date',
        'currency', 'original Amount Sum Claims',
    ]
    missing = [c for c in required if c not in df.columns]
    if missing:
        raise ValueError(f'Expected column(s) not found in "{SHEET_NAME}" sheet: {missing}')

    report_timestamp = pd.Timestamp.now().strftime('%d%m%Y%H%M')
    rows = []
    for i, r in df.iterrows():
        policy_number = _text_or_unknown(r['policy Number'])
        claim_ref_number = _text_or_unknown(r['claim Reference Number'])
        currency = str(r['currency']).strip().upper()
        org_amount = float(r['original Amount Sum Claims'])

        if currency != 'TZS':
            raise ValueError(
                f'Row {i} (policy {policy_number}) is in {currency}, not TZS - '
                'FX conversion to tzsSumClaimAmount is not implemented yet.'
            )
        tzs_amount = org_amount

        claim_date = _parse_date(r['claim Date'])
        incident_occurrence_date = _parse_date(r['incident Occurrence Date'])
        incident_reporting_date = _parse_date(r['incident Reporting Date'])

        # Natural key: policyNumber + claimRefNumber + claimDate, since
        # policyNumber alone can repeat 'Unknown' / claimRefNumber can repeat
        # 'Unknown' across different real claims.
        record_key = hashlib.sha256(
            '|'.join([policy_number, claim_ref_number, claim_date or '']).encode('utf-8')
        ).hexdigest()

        rows.append((
            record_key, report_timestamp, policy_number,
            'active',  # insurerStatus -- no real source, see module docstring
            claim_ref_number, claim_date, str(r['nature of Claim']).strip(),
            incident_occurrence_date, incident_reporting_date, currency,
            org_amount, tzs_amount,
            0, 0,  # orgPaidClaimAmount, tzsPaidClaimAmount -- no source
            'Open',  # claimStatus -- no source
            'Not Applicable',  # repudiationReason -- no source
            report_timestamp,  # claimClosureDate -- no source
        ))
    return rows


def replace_table(rows):
    setup_target_table()
    with postgres_connection() as connection, connection.cursor() as cursor:
        cursor.execute('DELETE FROM "insuranceClaim"')
        psycopg2.extras.execute_values(cursor, '''
            INSERT INTO "insuranceClaim" (
                "recordKey", "reportingDate", "policyNumber", "insurerStatus", "claimRefNumber",
                "claimDate", "claimNature", "incidentOccurrenceDate", "incidentReportingDate", currency,
                "orgSumClaimAmount", "tzsSumClaimAmount", "orgPaidClaimAmount", "tzsPaidClaimAmount",
                "claimStatus", "repudiationReason", "claimClosureDate"
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
    logger.info('insuranceClaim replaced from "%s": %s row(s) loaded.', sys.argv[1], len(loaded_rows))
