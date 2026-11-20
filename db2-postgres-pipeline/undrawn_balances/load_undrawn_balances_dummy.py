#!/usr/bin/env python3
"""Seed undrawnBalances with dummy data.

Per explicit business direction: there is no real source (PROFITS has no
interbank/counterparty undrawn-credit-line subsystem, and the business has
no spreadsheet for it either), so this report is populated with synthetic
placeholder rows rather than left empty, capped at 5 records - same
approach as the other dummy-seeded reports in this folder
(core_capital_deductions, dividends_payable, incoming_remittances,
outgoing_remittances).

Field names follow the "API Data Attribute" column of the business's own
"Undrawn Balances" spec image exactly: reportingDate,
customerIdentificationNumber, borrowerName, relationshipType, contractDate,
categoryUndrawnBalance, ratingStatus, crRatingCounterCustomer,
gradesUnratedCustomer, currency, orgSanctionedAmount, usdSanctionedAmount,
tzsSactionedAmount, orgDisbursedAmount, usdDisbursedAmount,
tzsDisbursedAmount, orgUnutilisedAmount, usdUnutilisedAmount,
tzsUnutilisedAmount, collateralType, pastDueDays, allowanceProbableLoss,
botProvision.

Note: tzsSactionedAmount is spelled exactly as the image gives it (missing
the 'n' in "Sanctioned") - the older Annexure 2 OpenAPI yaml (Ver03, Sept
2023) spells the equivalent field "tzsSanctionedAmount" instead. The image
is the current, more detailed spec the business is submitting against (as
established for the other dummy reports in this folder), so its literal
spelling is used here rather than "fixing" what may well be a live typo in
BoT's actual API.

This report is about undrawn credit lines extended to counterparty banks
and financial institutions (relationshipType/crRatingCounterCustomer/
gradesUnratedCustomer are all about rating the *counterparty*, not a
retail borrower), so borrowerName uses real Tanzanian/regional institution
names (from Annexure 5's "FSP List") rather than a fabricated person or
company - consistent with how dividends_payable/incoming_remittances reuse
real institution names for lookup-adjacent fields while keeping amounts
synthetic.

Every lookup-governed field uses a real BoT value (Annexure 5_Static and
LookUp Values Version03 Sept 2025_ FINAL.xlsx, sheet "Attribute Table
FINAL" unless noted):
  - relationshipType (D33 "Relationship type"): real item values -
    'Domestic banks unrelated', 'Foreign bank unrelated', 'Domestic
    Financial Institutions other than banks'.
  - categoryUndrawnBalance (D41 "Undrawn Balances"): all 4 real item
    values are used, one per row - 'Unexpired overdraft lines', 'Term
    loan and other advances', 'Unexpired mortgage credit lines' (used
    twice), 'Others'.
  - crRatingCounterCustomer (D67 "External Credit rating") /
    gradesUnratedCustomer (D68 "Grades of unrated exposures..."): paired
    correctly per the image's own Y/N rule - when ratingStatus is True,
    crRatingCounterCustomer is a real external rating band and
    gradesUnratedCustomer is left unset (not applicable to a rated
    counterparty); when ratingStatus is False, crRatingCounterCustomer is
    D67's own 'Unrated' item and gradesUnratedCustomer is set to a real
    D68 grade (A/B/C).
  - collateralType (D42 "Collateral Pledged"): real item values -
    'Unsecured', 'Deposits', 'Government Securities', 'MotorVehicle',
    'Residential mortgage'.
  - currency (sheet "Currencies"): TZS or USD, both real item values.

orgUnutilisedAmount/usdUnutilisedAmount/tzsUnutilisedAmount are kept
arithmetically consistent with orgSanctionedAmount - orgDisbursedAmount
(the literal definition of an undrawn balance), not independently
fabricated numbers.

contractDate is spread across the 5 months before reportingDate so it
never exceeds today, consistent with the project's "no future dates" rule
applied elsewhere.

Each run of this script REPLACES the entire contents of
"undrawnBalances" with exactly these 5 rows.

Usage:
    python load_undrawn_balances_dummy.py
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

# Illustrative-only FX rate (TZS per USD), used solely to make
# usd*/tzs* amounts internally consistent for this dummy data - not a
# live rate, since there is no real transaction behind any of these rows.
USD_TO_TZS = 2500.0

DUMMY_ROWS = [
    dict(
        client_file_number='CF-0001', months_ago=1,
        borrower_name='CRDB Bank Plc', relationship_type='Domestic banks unrelated',
        category_undrawn_balance='Unexpired overdraft lines',
        rating_status=True, cr_rating='A+ to A-', grades_unrated=None,
        currency='TZS', org_sanctioned=500000000.00, org_disbursed=300000000.00,
        collateral_type='Unsecured', past_due_days=0, allowance_probable_loss=0.00, bot_provision=0.00,
    ),
    dict(
        client_file_number='CF-0002', months_ago=2,
        borrower_name='Azania Bank Public Limited Company', relationship_type='Domestic banks unrelated',
        category_undrawn_balance='Term loan and other advances',
        rating_status=False, cr_rating='Unrated', grades_unrated='Grade B',
        currency='USD', org_sanctioned=200000.00, org_disbursed=150000.00,
        collateral_type='Deposits', past_due_days=0, allowance_probable_loss=0.00, bot_provision=0.00,
    ),
    dict(
        client_file_number='CF-0003', months_ago=3,
        borrower_name='Stanbic Bank Uganda', relationship_type='Foreign bank unrelated',
        category_undrawn_balance='Unexpired mortgage credit lines',
        rating_status=True, cr_rating='BBB+ to BBB-', grades_unrated=None,
        currency='USD', org_sanctioned=1000000.00, org_disbursed=700000.00,
        collateral_type='Government Securities', past_due_days=15, allowance_probable_loss=5000.00, bot_provision=5000.00,
    ),
    dict(
        client_file_number='CF-0004', months_ago=4,
        borrower_name='Equity Bank Kenya', relationship_type='Foreign bank unrelated',
        category_undrawn_balance='Others',
        rating_status=False, cr_rating='Unrated', grades_unrated='Grade C',
        currency='USD', org_sanctioned=150000.00, org_disbursed=90000.00,
        collateral_type='MotorVehicle', past_due_days=0, allowance_probable_loss=0.00, bot_provision=0.00,
    ),
    dict(
        client_file_number='CF-0005', months_ago=5,
        borrower_name='Tanzania Mortgage Refinance Company Ltd',
        relationship_type='Domestic Financial Institutions other than banks',
        category_undrawn_balance='Unexpired mortgage credit lines',
        rating_status=False, cr_rating='Unrated', grades_unrated='Grade A',
        currency='TZS', org_sanctioned=400000000.00, org_disbursed=250000000.00,
        collateral_type='Residential mortgage', past_due_days=0, allowance_probable_loss=0.00, bot_provision=0.00,
    ),
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
            CREATE TABLE IF NOT EXISTS "undrawnBalances" (
                id BIGSERIAL PRIMARY KEY,
                "recordKey" VARCHAR(64) UNIQUE,
                "reportingDate" VARCHAR(12), "customerIdentificationNumber" VARCHAR(100),
                "borrowerName" VARCHAR(255), "relationshipType" VARCHAR(255), "contractDate" VARCHAR(12),
                "categoryUndrawnBalance" VARCHAR(255), "ratingStatus" BOOLEAN,
                "crRatingCounterCustomer" VARCHAR(100), "gradesUnratedCustomer" VARCHAR(50), currency VARCHAR(20),
                "orgSanctionedAmount" NUMERIC(31, 2), "usdSanctionedAmount" NUMERIC(31, 2), "tzsSactionedAmount" NUMERIC(31, 2),
                "orgDisbursedAmount" NUMERIC(31, 2), "usdDisbursedAmount" NUMERIC(31, 2), "tzsDisbursedAmount" NUMERIC(31, 2),
                "orgUnutilisedAmount" NUMERIC(31, 2), "usdUnutilisedAmount" NUMERIC(31, 2), "tzsUnutilisedAmount" NUMERIC(31, 2),
                "collateralType" VARCHAR(255), "pastDueDays" NUMERIC(31, 0),
                "allowanceProbableLoss" NUMERIC(31, 2), "botProvision" NUMERIC(31, 2)
            )
        ''')
        cursor.execute('CREATE UNIQUE INDEX IF NOT EXISTS idx_undrawnBalances_record_key ON "undrawnBalances" ("recordKey")')
        connection.commit()


def _convert(org_amount, currency):
    if currency == 'TZS':
        return org_amount / USD_TO_TZS, org_amount
    if currency == 'USD':
        return org_amount, org_amount * USD_TO_TZS
    raise ValueError(f'No FX handling defined for currency {currency!r}')


def build_rows():
    now = pd.Timestamp.now()
    report_timestamp = now.strftime('%d%m%Y%H%M')
    rows = []
    for r in DUMMY_ROWS:
        contract_date = (now - pd.DateOffset(months=r['months_ago'])).strftime('%d%m%Y%H%M')
        currency = r['currency']

        org_sanctioned = r['org_sanctioned']
        org_disbursed = r['org_disbursed']
        org_unutilised = round(org_sanctioned - org_disbursed, 2)

        usd_sanctioned, tzs_sanctioned = _convert(org_sanctioned, currency)
        usd_disbursed, tzs_disbursed = _convert(org_disbursed, currency)
        usd_unutilised, tzs_unutilised = _convert(org_unutilised, currency)

        record_key = hashlib.sha256(
            '|'.join([r['client_file_number'], contract_date]).encode('utf-8')
        ).hexdigest()

        rows.append((
            record_key, report_timestamp, r['client_file_number'], r['borrower_name'],
            r['relationship_type'], contract_date, r['category_undrawn_balance'],
            r['rating_status'], r['cr_rating'], r['grades_unrated'], currency,
            org_sanctioned, round(usd_sanctioned, 2), round(tzs_sanctioned, 2),
            org_disbursed, round(usd_disbursed, 2), round(tzs_disbursed, 2),
            org_unutilised, round(usd_unutilised, 2), round(tzs_unutilised, 2),
            r['collateral_type'], r['past_due_days'], r['allowance_probable_loss'], r['bot_provision'],
        ))
    return rows


def replace_table(rows):
    setup_target_table()
    with postgres_connection() as connection, connection.cursor() as cursor:
        cursor.execute('DELETE FROM "undrawnBalances"')
        psycopg2.extras.execute_values(cursor, '''
            INSERT INTO "undrawnBalances" (
                "recordKey", "reportingDate", "customerIdentificationNumber", "borrowerName",
                "relationshipType", "contractDate", "categoryUndrawnBalance",
                "ratingStatus", "crRatingCounterCustomer", "gradesUnratedCustomer", currency,
                "orgSanctionedAmount", "usdSanctionedAmount", "tzsSactionedAmount",
                "orgDisbursedAmount", "usdDisbursedAmount", "tzsDisbursedAmount",
                "orgUnutilisedAmount", "usdUnutilisedAmount", "tzsUnutilisedAmount",
                "collateralType", "pastDueDays", "allowanceProbableLoss", "botProvision"
            ) VALUES %s
        ''', rows)
        connection.commit()


if __name__ == '__main__':
    logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
    seeded_rows = build_rows()
    replace_table(seeded_rows)
    logger.info('undrawnBalances replaced with %s dummy row(s).', len(seeded_rows))
