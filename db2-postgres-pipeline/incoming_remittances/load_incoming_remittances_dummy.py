#!/usr/bin/env python3
"""Seed incomingRemittances with dummy data.

Per explicit business direction: there is no real source (PROFITS has no
incoming-remittances subsystem, and the business has no spreadsheet for it
either), so this report is populated with synthetic placeholder rows rather
than left empty, capped at 5 records - same approach as
core_capital_deductions/load_core_capital_deductions_dummy.py and
dividends_payable/load_dividends_payable_dummy.py.

Field names follow the "API Data Attribute" column of the business's own
"Incoming - Remittances" spec image exactly (reportingDate, transactionId,
transactionDate, remittanceTransactionType, remittanceType,
remittanceSubType, senderName, senderCountry, senderPostalCode,
senderHouseNumber, senderIdentification, senderIdentificationNumber,
mobileNumber, transferChannel, subCategoryTransferChannel, recipientName,
recipientCountry, recipientRegion, recipientDistrict, recipientWard,
recipientStreet, recipientHouseNumber, recipientPostalCode,
recipientIdentificationType, recipientIdentificationNumber,
recipientAccountNumber, recipientMobileNumber, terminatingChannel,
terminatingChannelName, currency, orgAmount, usdAmount, tzsAmount,
agentPrincipal). The older Annexure 2 OpenAPI yaml (Ver03, Sept 2023)
models a couple of these as nested sender/recipient-address sub-objects
instead of flat fields, and omits senderIdentificationNumber /
recipientIdentificationType outright, but the image is the current,
more detailed spec the business is submitting against, so its flat field
list is what this table/script follow.

Every lookup-governed field uses a real BoT value (Annexure 5_Static and
LookUp Values Version03 Sept 2025_ FINAL.xlsx, sheet "Attribute Table
FINAL" unless noted):
  - remittanceTransactionType (D114 "Transaction Type"): always 'Inflow' -
    this report is inherently inbound.
  - remittanceType / remittanceSubType (D71 "Remittance type"): D71 is
    hierarchical - only item 1, 'Personal transfers (part of current
    transfers)', has real sub-items (House Consumption, School fees, ...);
    the other 4 top-level items (Compensation of employees, Capital
    transfers between households, Social contribution, Social benefits)
    have none, so remittanceSubType is left unset for those rows,
    consistent with remittanceSubType being optional (N) in the spec.
  - transferChannel / subCategoryTransferChannel (D75 "Transfer channel"):
    only channel/subcategory pairs actually listed together in the lookup
    are used - 'Money Transfer Operators' with one of its named operators
    (Western Union / MoneyGram / World Remit), or 'TIPS' with its own
    'Banks'/'Mobile' subcategories.
  - senderIdentification / recipientIdentificationType (D02
    "Indentification type"): real item values (e.g. 'Passport',
    'NationalIdentityCard').
  - terminatingChannel (D11 "Remittance Terminating Channel") and
    terminatingChannelName (sheet "FSP List", per the image's own
    highlighted rule): real Tanzanian FSP entries - banks (Absa, CRDB,
    Azania, Diamond Trust Bank) when terminatingChannel is 'Banks and
    financial institution', or the MNO 'M-Pesa Limited' when it is
    'Mobile Network Operators'.
  - agentPrincipal (D115): 'bank' for the bank-terminated rows, 'mobilemoney'
    for the MNO-terminated one.
  - senderCountry / recipientCountry (sheet "Country codes", ISO 3166-1):
    recipientCountry is always 'TZ'; senderCountry varies across real
    codes (KE, US, GB, UG, ZA).
  - recipientRegion / recipientDistrict / recipientWard (sheet "Regions -
    Location"): a real, existing combination - Dar es Salaam / Ubungo /
    one of its real wards.
  - currency (sheet "Currencies"): USD or GBP, both real item values.

recipientAccountNumber follows the same 10-digit numeric format used in
"accountInformation" when terminatingChannel is a bank (mandatory per the
image's own business rule), and '0' - the image's own documented
convention for "cash pickup at counter/collection" - for the one
MNO-terminated row. usdAmount/tzsAmount use a single illustrative FX rate
(not a live one, since there is no real transaction to convert) and are
clearly fictitious like every other amount in this table.

senderName/recipientName/senderIdentificationNumber/
recipientIdentificationNumber/recipientMobileNumber/mobileNumber have no
governing lookup, so they use plausible-looking but obviously-synthetic
values, not drawn from any real customer or counterparty record.

transactionDate is spread across the 5 months before reportingDate so it
never exceeds today, consistent with the project's "no future dates" rule
applied elsewhere.

Each run of this script REPLACES the entire contents of
"incomingRemittances" with exactly these 5 rows.

Usage:
    python load_incoming_remittances_dummy.py
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

# Illustrative-only FX rates (TZS per unit), used solely to make
# usdAmount/tzsAmount internally consistent for this dummy data - not a
# live rate, since there is no real transaction behind any of these rows.
USD_TO_TZS = 2500.0
GBP_TO_USD = 1.27

DUMMY_ROWS = [
    dict(
        transaction_id='REM-0001', months_ago=1,
        remittance_type='Personal transfers (part of current transfers)', remittance_sub_type='House Consumption',
        sender_name='John Mwangi', sender_country='KE', sender_postal_code='100', sender_house_number='12',
        sender_identification='Passport', sender_identification_number='KEPPT100001',
        mobile_number='254712345001',
        transfer_channel='Money Transfer Operators', sub_category_transfer_channel='Western Union',
        recipient_name='Hassan Juma', recipient_region='Dar es Salaam', recipient_district='Ubungo',
        recipient_ward='Kimara', recipient_street='Kimara Mwisho', recipient_house_number='15',
        recipient_postal_code='14110', recipient_identification_type='NationalIdentityCard',
        recipient_identification_number='19900101100001', recipient_account_number='0002000001',
        recipient_mobile_number='255712345101',
        terminating_channel='Banks and financial institution', terminating_channel_name='Absa Bank Tanzania Limited',
        currency='USD', org_amount=500.00, agent_principal='bank',
    ),
    dict(
        transaction_id='REM-0002', months_ago=2,
        remittance_type='Personal transfers (part of current transfers)', remittance_sub_type='School fees',
        sender_name='Grace Otieno', sender_country='US', sender_postal_code='10001', sender_house_number='221',
        sender_identification='Passport', sender_identification_number='USPPT100002',
        mobile_number='12125550102',
        transfer_channel='Money Transfer Operators', sub_category_transfer_channel='MoneyGram',
        recipient_name='Fatuma Said', recipient_region='Dar es Salaam', recipient_district='Ubungo',
        recipient_ward='Kibamba', recipient_street='Kibamba Mloganzila', recipient_house_number='22',
        recipient_postal_code='14111', recipient_identification_type='Passport',
        recipient_identification_number='T1002345', recipient_account_number='0002000002',
        recipient_mobile_number='255712345102',
        terminating_channel='Banks and financial institution', terminating_channel_name='CRDB Bank Plc',
        currency='USD', org_amount=800.00, agent_principal='bank',
    ),
    dict(
        transaction_id='REM-0003', months_ago=3,
        remittance_type='Compensation of employees', remittance_sub_type=None,
        sender_name='Michael Smith', sender_country='GB', sender_postal_code='SW1A', sender_house_number='10',
        sender_identification='DrivingLicense', sender_identification_number='GBDL100003',
        mobile_number='447700900103',
        transfer_channel='Money Transfer Operators', sub_category_transfer_channel='World Remit',
        recipient_name='Peter Mushi', recipient_region='Dar es Salaam', recipient_district='Ubungo',
        recipient_ward='Kimara', recipient_street='Mabibo External', recipient_house_number='8',
        recipient_postal_code='14112', recipient_identification_type='VotersRegistrationCard',
        recipient_identification_number='V1003456', recipient_account_number='0002000003',
        recipient_mobile_number='255712345103',
        terminating_channel='Banks and financial institution', terminating_channel_name='Azania Bank Public Limited Company',
        currency='GBP', org_amount=300.00, agent_principal='bank',
    ),
    dict(
        transaction_id='REM-0004', months_ago=4,
        remittance_type='Capital transfers between households', remittance_sub_type=None,
        sender_name='Robert Okello', sender_country='UG', sender_postal_code='256', sender_house_number='7',
        sender_identification='NationalIdentityCard', sender_identification_number='UGID100004',
        mobile_number='256772345104',
        transfer_channel='TIPS', sub_category_transfer_channel='Banks',
        recipient_name='Rehema Kway', recipient_region='Dar es Salaam', recipient_district='Ubungo',
        recipient_ward='Mbezi', recipient_street='Mbezi Beach', recipient_house_number='31',
        recipient_postal_code='14113', recipient_identification_type='DrivingLicense',
        recipient_identification_number='D1004567', recipient_account_number='0002000004',
        recipient_mobile_number='255712345104',
        terminating_channel='Banks and financial institution', terminating_channel_name='Diamond Trust Bank Tanzania Public Limited Company',
        currency='USD', org_amount=200.00, agent_principal='bank',
    ),
    dict(
        transaction_id='REM-0005', months_ago=5,
        remittance_type='Social benefits', remittance_sub_type=None,
        sender_name='Amara Dlamini', sender_country='ZA', sender_postal_code='2000', sender_house_number='4',
        sender_identification='Passport', sender_identification_number='ZAPPT100005',
        mobile_number='27823450105',
        transfer_channel='TIPS', sub_category_transfer_channel='Mobile',
        recipient_name='Daudi Komba', recipient_region='Dar es Salaam', recipient_district='Ubungo',
        recipient_ward='Goba', recipient_street='Goba Station', recipient_house_number='3',
        recipient_postal_code='14114', recipient_identification_type='ZanzibarIdentityCard',
        recipient_identification_number='Z1005678', recipient_account_number='0',
        recipient_mobile_number='255712345105',
        terminating_channel='Mobile Network Operators', terminating_channel_name='M-Pesa Limited',
        currency='USD', org_amount=150.00, agent_principal='mobilemoney',
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
            CREATE TABLE IF NOT EXISTS "incomingRemittances" (
                id BIGSERIAL PRIMARY KEY,
                "recordKey" VARCHAR(64) UNIQUE,
                "reportingDate" VARCHAR(12), "transactionId" VARCHAR(100), "transactionDate" VARCHAR(12),
                "remittanceTransactionType" VARCHAR(50), "remittanceType" VARCHAR(255), "remittanceSubType" VARCHAR(255),
                "senderName" VARCHAR(255), "senderCountry" VARCHAR(10), "senderPostalCode" VARCHAR(20),
                "senderHouseNumber" VARCHAR(20), "senderIdentification" VARCHAR(100), "senderIdentificationNumber" VARCHAR(100),
                "mobileNumber" VARCHAR(20), "transferChannel" VARCHAR(255), "subCategoryTransferChannel" VARCHAR(255),
                "recipientName" VARCHAR(255), "recipientCountry" VARCHAR(10), "recipientRegion" VARCHAR(100),
                "recipientDistrict" VARCHAR(100), "recipientWard" VARCHAR(100), "recipientStreet" VARCHAR(255),
                "recipientHouseNumber" VARCHAR(20), "recipientPostalCode" VARCHAR(20),
                "recipientIdentificationType" VARCHAR(100), "recipientIdentificationNumber" VARCHAR(100),
                "recipientAccountNumber" VARCHAR(50), "recipientMobileNumber" VARCHAR(20),
                "terminatingChannel" VARCHAR(100), "terminatingChannelName" VARCHAR(255), currency VARCHAR(20),
                "orgAmount" NUMERIC(31, 2), "usdAmount" NUMERIC(31, 2), "tzsAmount" NUMERIC(31, 2),
                "agentPrincipal" VARCHAR(50)
            )
        ''')
        cursor.execute('CREATE UNIQUE INDEX IF NOT EXISTS idx_incomingRemittances_record_key ON "incomingRemittances" ("recordKey")')
        connection.commit()


def build_rows():
    now = pd.Timestamp.now()
    report_timestamp = now.strftime('%d%m%Y%H%M')
    rows = []
    for r in DUMMY_ROWS:
        transaction_date = (now - pd.DateOffset(months=r['months_ago'])).strftime('%d%m%Y%H%M')
        currency = r['currency']
        org_amount = r['org_amount']

        if currency == 'USD':
            usd_amount = org_amount
            tzs_amount = org_amount * USD_TO_TZS
        elif currency == 'GBP':
            usd_amount = round(org_amount * GBP_TO_USD, 2)
            tzs_amount = round(usd_amount * USD_TO_TZS, 2)
        else:
            raise ValueError(f'No FX handling defined for currency {currency!r}')

        record_key = hashlib.sha256(r['transaction_id'].encode('utf-8')).hexdigest()

        rows.append((
            record_key, report_timestamp, r['transaction_id'], transaction_date,
            'Inflow', r['remittance_type'], r['remittance_sub_type'],
            r['sender_name'], r['sender_country'], r['sender_postal_code'], r['sender_house_number'],
            r['sender_identification'], r['sender_identification_number'],
            r['mobile_number'], r['transfer_channel'], r['sub_category_transfer_channel'],
            r['recipient_name'], 'TZ', r['recipient_region'], r['recipient_district'], r['recipient_ward'],
            r['recipient_street'], r['recipient_house_number'], r['recipient_postal_code'],
            r['recipient_identification_type'], r['recipient_identification_number'],
            r['recipient_account_number'], r['recipient_mobile_number'],
            r['terminating_channel'], r['terminating_channel_name'], currency,
            org_amount, usd_amount, tzs_amount, r['agent_principal'],
        ))
    return rows


def replace_table(rows):
    setup_target_table()
    with postgres_connection() as connection, connection.cursor() as cursor:
        cursor.execute('DELETE FROM "incomingRemittances"')
        psycopg2.extras.execute_values(cursor, '''
            INSERT INTO "incomingRemittances" (
                "recordKey", "reportingDate", "transactionId", "transactionDate",
                "remittanceTransactionType", "remittanceType", "remittanceSubType",
                "senderName", "senderCountry", "senderPostalCode", "senderHouseNumber",
                "senderIdentification", "senderIdentificationNumber",
                "mobileNumber", "transferChannel", "subCategoryTransferChannel",
                "recipientName", "recipientCountry", "recipientRegion", "recipientDistrict", "recipientWard",
                "recipientStreet", "recipientHouseNumber", "recipientPostalCode",
                "recipientIdentificationType", "recipientIdentificationNumber",
                "recipientAccountNumber", "recipientMobileNumber",
                "terminatingChannel", "terminatingChannelName", currency,
                "orgAmount", "usdAmount", "tzsAmount", "agentPrincipal"
            ) VALUES %s
        ''', rows)
        connection.commit()


if __name__ == '__main__':
    logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
    seeded_rows = build_rows()
    replace_table(seeded_rows)
    logger.info('incomingRemittances replaced with %s dummy row(s).', len(seeded_rows))
