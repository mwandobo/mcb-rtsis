#!/usr/bin/env python3
"""Seed otherLiabilities with dummy data.

Per explicit business direction: there is no real source (PROFITS has no
other-liabilities subsystem, and the business has no spreadsheet for it
either), so this report is populated with synthetic placeholder rows rather
than left empty, capped at 5 records - same approach as
core_capital_deductions/dividends_payable/undrawn_balances/
incoming_remittances/outgoing_remittances.

Field names follow the "API specifications" column of the business's own
"Other liabilities" spec image exactly (reportingDate, liabilityCategory,
counterpartyName, counterpartyCountry, transactionDate, valueDate,
maturityDate, currency, orgAmountOpening, usdAmountOpening,
tzsAmountOpening, orgAmountPayment, usdAmountPayment, tzsAmountPayment,
orgAmountBalance, usdAmountBalance, tzsAmountBalance,
sectorSnaClassification).

Every lookup-governed field uses a real BoT value (Annexure 5_Static and
LookUp Values Version03 Sept 2025_ FINAL.xlsx):
  - liabilityCategory (sheet "Attribute Table FINAL", D36 "Other
    Liabilities", item codes 1-15): 5 of the 15 real item values are used,
    one per row - 'Accounts payable', 'Benefits payable', 'Dividends
    payable', 'Withholding tax payable', 'Tax Payable'.
  - counterpartyCountry (sheet "Country codes", ISO 3166-1): the sheet's
    "Description" column (the country name, not the "Item value" 2-letter
    code) - 'KENYA' for the one foreign-vendor row (a Kenyan equipment
    supplier is a plausible foreign accounts-payable counterparty for a
    bank), 'TANZANIA, UNITED REPUBLIC OF' (the sheet's exact spelling) for
    the 4 domestic counterparties (retiree, shareholder, tax authority
    twice).
  - currency (sheet "Currencies"): 'USD' for the foreign vendor row, 'TZS'
    for the domestic rows - both real item values.
  - sectorSnaClassification (sheet "EconomicSector_SectorSNAClassi"): real
    item values matched to each counterparty's actual nature - 'Other
    Non-Financial Corporations' (the equipment vendor), 'Households' (the
    retired employee and the individual shareholder), 'General government'
    (Tanzania Revenue Authority, twice).

counterpartyName/orgAmount* have no governing lookup, so they use
plausible-looking but obviously-synthetic values, not drawn from any real
customer or counterparty record. orgAmountBalance is kept internally
consistent as orgAmountOpening - orgAmountPayment for every row (including
the one fully-repaid benefits-payable row, where the balance is zero, and
the one not-yet-remitted withholding-tax row, where payment is zero and
balance equals opening - both realistic states for a "payable" liability).

transactionDate is spread across the months before reportingDate (never
exceeding today, consistent with the project's "no future dates" rule
applied elsewhere); valueDate is set equal to transactionDate (liability
recognised the same day it arises, absent clearer guidance); maturityDate
is set 30 days after transactionDate, which still stays in the past for
every row given the months_ago spacing used here.

Each run of this script REPLACES the entire contents of
"otherLiabilities" with exactly these 5 rows.

Usage:
    python load_other_liabilities_dummy.py
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
# usdAmount*/tzsAmount* internally consistent for this dummy data - not a
# live rate, since there is no real transaction behind any of these rows.
USD_TO_TZS = 2500.0

DUMMY_ROWS = [
    dict(
        liability_category='Accounts payable', months_ago=3,
        counterparty_name='Nairobi Business Systems Ltd', counterparty_country='KENYA',
        currency='USD', org_opening=15000.00, org_payment=5000.00,
        sector_sna='Other Non-Financial Corporations',
    ),
    dict(
        liability_category='Benefits payable', months_ago=2,
        counterparty_name='Juma Ally Mwakalinga', counterparty_country='TANZANIA, UNITED REPUBLIC OF',
        currency='TZS', org_opening=8000000.00, org_payment=8000000.00,
        sector_sna='Households',
    ),
    dict(
        liability_category='Dividends payable', months_ago=4,
        counterparty_name='Neema Peter Kessy', counterparty_country='TANZANIA, UNITED REPUBLIC OF',
        currency='TZS', org_opening=25000000.00, org_payment=10000000.00,
        sector_sna='Households',
    ),
    dict(
        liability_category='Withholding tax payable', months_ago=2,
        counterparty_name='Tanzania Revenue Authority', counterparty_country='TANZANIA, UNITED REPUBLIC OF',
        currency='TZS', org_opening=4500000.00, org_payment=0.00,
        sector_sna='General government',
    ),
    dict(
        liability_category='Tax Payable', months_ago=5,
        counterparty_name='Tanzania Revenue Authority', counterparty_country='TANZANIA, UNITED REPUBLIC OF',
        currency='TZS', org_opening=60000000.00, org_payment=20000000.00,
        sector_sna='General government',
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
            CREATE TABLE IF NOT EXISTS "otherLiabilities" (
                id BIGSERIAL PRIMARY KEY,
                "recordKey" VARCHAR(64) UNIQUE,
                "reportingDate" VARCHAR(12), "liabilityCategory" VARCHAR(255),
                "counterpartyName" VARCHAR(255), "counterpartyCountry" VARCHAR(100),
                "transactionDate" VARCHAR(12), "valueDate" VARCHAR(12), "maturityDate" VARCHAR(12),
                currency VARCHAR(20),
                "orgAmountOpening" NUMERIC(31, 2), "usdAmountOpening" NUMERIC(31, 2), "tzsAmountOpening" NUMERIC(31, 2),
                "orgAmountPayment" NUMERIC(31, 2), "usdAmountPayment" NUMERIC(31, 2), "tzsAmountPayment" NUMERIC(31, 2),
                "orgAmountBalance" NUMERIC(31, 2), "usdAmountBalance" NUMERIC(31, 2), "tzsAmountBalance" NUMERIC(31, 2),
                "sectorSnaClassification" VARCHAR(255)
            )
        ''')
        cursor.execute('CREATE UNIQUE INDEX IF NOT EXISTS idx_otherLiabilities_record_key ON "otherLiabilities" ("recordKey")')
        # Widen counterpartyCountry for tables created before this column held
        # full country names instead of 2-letter codes; no-op once already sized.
        cursor.execute('ALTER TABLE "otherLiabilities" ALTER COLUMN "counterpartyCountry" TYPE VARCHAR(100)')
        connection.commit()


def to_usd(amount, currency):
    return amount if currency == 'USD' else round(amount / USD_TO_TZS, 2)


def to_tzs(amount, currency):
    return round(amount * USD_TO_TZS, 2) if currency == 'USD' else amount


def build_rows():
    now = pd.Timestamp.now()
    report_timestamp = now.strftime('%d%m%Y%H%M')
    rows = []
    for r in DUMMY_ROWS:
        transaction_date = now - pd.DateOffset(months=r['months_ago'])
        value_date = transaction_date
        maturity_date = transaction_date + pd.DateOffset(days=30)

        currency = r['currency']
        org_opening = r['org_opening']
        org_payment = r['org_payment']
        org_balance = round(org_opening - org_payment, 2)

        record_key = hashlib.sha256(
            '|'.join([r['liability_category'], r['counterparty_name'], transaction_date.isoformat()]).encode('utf-8')
        ).hexdigest()

        rows.append((
            record_key, report_timestamp, r['liability_category'],
            r['counterparty_name'], r['counterparty_country'],
            transaction_date.strftime('%d%m%Y%H%M'), value_date.strftime('%d%m%Y%H%M'), maturity_date.strftime('%d%m%Y%H%M'),
            currency,
            org_opening, to_usd(org_opening, currency), to_tzs(org_opening, currency),
            org_payment, to_usd(org_payment, currency), to_tzs(org_payment, currency),
            org_balance, to_usd(org_balance, currency), to_tzs(org_balance, currency),
            r['sector_sna'],
        ))
    return rows


def replace_table(rows):
    setup_target_table()
    with postgres_connection() as connection, connection.cursor() as cursor:
        cursor.execute('DELETE FROM "otherLiabilities"')
        psycopg2.extras.execute_values(cursor, '''
            INSERT INTO "otherLiabilities" (
                "recordKey", "reportingDate", "liabilityCategory",
                "counterpartyName", "counterpartyCountry",
                "transactionDate", "valueDate", "maturityDate",
                currency,
                "orgAmountOpening", "usdAmountOpening", "tzsAmountOpening",
                "orgAmountPayment", "usdAmountPayment", "tzsAmountPayment",
                "orgAmountBalance", "usdAmountBalance", "tzsAmountBalance",
                "sectorSnaClassification"
            ) VALUES %s
        ''', rows)
        connection.commit()


if __name__ == '__main__':
    logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
    seeded_rows = build_rows()
    replace_table(seeded_rows)
    logger.info('otherLiabilities replaced with %s dummy row(s).', len(seeded_rows))
