#!/usr/bin/env python3
"""Seed outgoingRemittances with dummy data.

Per explicit business direction: there is no real source (PROFITS has no
outgoing-remittances subsystem, and the business has no spreadsheet for it
either), so this report is populated with synthetic placeholder rows rather
than left empty, capped at 5 records - same approach as
incoming_remittances/load_incoming_remittances_dummy.py (its mirror-image
report: sender is domestic here, recipient is abroad, instead of the
other way round).

Field names follow the "API Data Attribute" column of the business's own
"Outgoing - Remittances" spec image exactly (reportingDate, transactionId,
transactionDate, remittanceTransactionType, remittanceType,
remittanceSubType, senderName, senderRegion, senderDistrict, senderWard,
senderStreet, senderHouseNumber, senderPostalCode, senderCountry,
senderIdentificationType, senderIdentificationNumber, mobileNumber,
transferChannel, subCategoryTransferChannel, recipientName,
recipientCountry, recipientPostalCode, recipientHouseNumber,
terminatingChannel, terminatingChannelName, recipientAccountNumber,
recipientMobileNumber, currency, orgAmount, usdAmount, tzsAmount,
agentPrincipal) - confirmed consistent with the senderPhysicalAddress /
recipientAddress sub-objects in the older Annexure 2 OpenAPI yaml
(Ver03, Sept 2023), just flattened the same way as incoming_remittances.

Every lookup-governed field uses a real BoT value (Annexure 5_Static and
LookUp Values Version03 Sept 2025_ FINAL.xlsx, sheet "Attribute Table
FINAL" unless noted):
  - remittanceTransactionType (D114 "Transaction Type"): always 'outflow'
    (the sheet's own item-2 value, lowercase) - this report is inherently
    outbound.
  - remittanceType / remittanceSubType (D71 "Remittance type"): same
    hierarchy rule as incoming_remittances - only 'Personal transfers
    (part of current transfers)' has real sub-items, so remittanceSubType
    is left unset for the other 3 top-level types used here.
  - senderCountry: always 'TZ' per the image's own business rule
    ("Select Tanzania for outgoing transfers").
  - senderIdentificationType (D02 "Indentification type") and
    senderRegion/senderDistrict/senderWard (sheet "Regions - Location"):
    real item values - Dar es Salaam / Ubungo / one of its real wards,
    mirroring the ward choices used in incoming_remittances.
  - transferChannel / subCategoryTransferChannel (D75 "Transfer channel"):
    same validated pairs as incoming_remittances - 'Money Transfer
    Operators' with a named operator, or 'TIPS' with 'Banks'/'Mobile'.
  - recipientCountry (sheet "Country codes", ISO 3166-1): real codes
    (KE, US, GB, UG, ZA), mirroring incoming_remittances' sender
    countries.
  - terminatingChannel (D11 "Remittance Terminating Channel") and
    agentPrincipal (D115): real item values. Unlike incoming_remittances,
    the image marks terminatingChannelName here as plain "text or
    alphanumeric" (no FSP List reference), since the receiving
    institution is foreign and outside that Tanzania-only lookup - so
    these use plausible, clearly-illustrative foreign institution names
    instead.
  - currency (sheet "Currencies"): USD or GBP, both real item values.

recipientAccountNumber/recipientMobileNumber follow the image's own
mandatory/optional rule: an account number is only set for the
bank-terminated rows (recipientMobileNumber left unset there, since it's
only mandatory for MNO/mobile-phone termination), and '0' plus a real
recipientMobileNumber for the one MNO-terminated row.

senderName/recipientName/senderIdentificationNumber/mobileNumber have no
governing lookup, so they use plausible-looking but obviously-synthetic
values, not drawn from any real customer or counterparty record.

transactionDate is spread across the 5 months before reportingDate so it
never exceeds today, consistent with the project's "no future dates" rule
applied elsewhere.

Each run of this script REPLACES the entire contents of
"outgoingRemittances" with exactly these 5 rows.

Usage:
    python load_outgoing_remittances_dummy.py
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
        transaction_id='REMOUT-0001', months_ago=1,
        remittance_type='Personal transfers (part of current transfers)', remittance_sub_type='House Consumption',
        sender_name='Imani Massawe', sender_region='Dar es Salaam', sender_district='Ubungo', sender_ward='Kimara',
        sender_street='Kimara Mwisho', sender_house_number='15', sender_postal_code='14110',
        sender_identification_type='NationalIdentityCard', sender_identification_number='19900101100001',
        mobile_number='255712345101',
        transfer_channel='Money Transfer Operators', sub_category_transfer_channel='Western Union',
        recipient_name='John Mwangi', recipient_country='KE',
        recipient_postal_code='100', recipient_house_number='12',
        terminating_channel='Banks and financial institution', terminating_channel_name='Equity Bank Kenya',
        recipient_account_number='KE0102030401', recipient_mobile_number=None,
        currency='USD', org_amount=500.00, agent_principal='bank',
    ),
    dict(
        transaction_id='REMOUT-0002', months_ago=2,
        remittance_type='Personal transfers (part of current transfers)', remittance_sub_type='School fees',
        sender_name='Agnes Mollel', sender_region='Dar es Salaam', sender_district='Ubungo', sender_ward='Kibamba',
        sender_street='Kibamba Mloganzila', sender_house_number='22', sender_postal_code='14111',
        sender_identification_type='Passport', sender_identification_number='T1100002',
        mobile_number='255712345102',
        transfer_channel='Money Transfer Operators', sub_category_transfer_channel='MoneyGram',
        recipient_name='Grace Otieno', recipient_country='US',
        recipient_postal_code='10001', recipient_house_number='221',
        terminating_channel='Banks and financial institution', terminating_channel_name='Bank of America',
        recipient_account_number='US0203040502', recipient_mobile_number=None,
        currency='USD', org_amount=800.00, agent_principal='bank',
    ),
    dict(
        transaction_id='REMOUT-0003', months_ago=3,
        remittance_type='Compensation of employees', remittance_sub_type=None,
        sender_name='Shabani Rajabu', sender_region='Dar es Salaam', sender_district='Ubungo', sender_ward='Kimara',
        sender_street='Mabibo External', sender_house_number='8', sender_postal_code='14112',
        sender_identification_type='DrivingLicense', sender_identification_number='D1100003',
        mobile_number='255712345103',
        transfer_channel='Money Transfer Operators', sub_category_transfer_channel='World Remit',
        recipient_name='Michael Smith', recipient_country='GB',
        recipient_postal_code='SW1A', recipient_house_number='10',
        terminating_channel='Banks and financial institution', terminating_channel_name='Barclays Bank UK',
        recipient_account_number='GB0304050603', recipient_mobile_number=None,
        currency='GBP', org_amount=300.00, agent_principal='bank',
    ),
    dict(
        transaction_id='REMOUT-0004', months_ago=4,
        remittance_type='Capital transfers between households', remittance_sub_type=None,
        sender_name='Winnie Kapinga', sender_region='Dar es Salaam', sender_district='Ubungo', sender_ward='Mbezi',
        sender_street='Mbezi Beach', sender_house_number='31', sender_postal_code='14113',
        sender_identification_type='VotersRegistrationCard', sender_identification_number='V1100004',
        mobile_number='255712345104',
        transfer_channel='TIPS', sub_category_transfer_channel='Banks',
        recipient_name='Robert Okello', recipient_country='UG',
        recipient_postal_code='256', recipient_house_number='7',
        terminating_channel='Banks and financial institution', terminating_channel_name='Stanbic Bank Uganda',
        recipient_account_number='UG0405060704', recipient_mobile_number=None,
        currency='USD', org_amount=200.00, agent_principal='bank',
    ),
    dict(
        transaction_id='REMOUT-0005', months_ago=5,
        remittance_type='Social benefits', remittance_sub_type=None,
        sender_name='Yusuf Mbwana', sender_region='Dar es Salaam', sender_district='Ubungo', sender_ward='Goba',
        sender_street='Goba Station', sender_house_number='3', sender_postal_code='14114',
        sender_identification_type='Passport', sender_identification_number='T1100005',
        mobile_number='255712345105',
        transfer_channel='TIPS', sub_category_transfer_channel='Mobile',
        recipient_name='Amara Dlamini', recipient_country='ZA',
        recipient_postal_code='2000', recipient_house_number='4',
        terminating_channel='Mobile Network Operators', terminating_channel_name='MTN Mobile Money South Africa',
        recipient_account_number='0', recipient_mobile_number='27823450105',
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
            CREATE TABLE IF NOT EXISTS "outgoingRemittances" (
                id BIGSERIAL PRIMARY KEY,
                "recordKey" VARCHAR(64) UNIQUE,
                "reportingDate" VARCHAR(12), "transactionId" VARCHAR(100), "transactionDate" VARCHAR(12),
                "remittanceTransactionType" VARCHAR(50), "remittanceType" VARCHAR(255), "remittanceSubType" VARCHAR(255),
                "senderName" VARCHAR(255), "senderRegion" VARCHAR(100), "senderDistrict" VARCHAR(100),
                "senderWard" VARCHAR(100), "senderStreet" VARCHAR(255), "senderHouseNumber" VARCHAR(20),
                "senderPostalCode" VARCHAR(20), "senderCountry" VARCHAR(10),
                "senderIdentificationType" VARCHAR(100), "senderIdentificationNumber" VARCHAR(100),
                "mobileNumber" VARCHAR(20), "transferChannel" VARCHAR(255), "subCategoryTransferChannel" VARCHAR(255),
                "recipientName" VARCHAR(255), "recipientCountry" VARCHAR(10), "recipientPostalCode" VARCHAR(20),
                "recipientHouseNumber" VARCHAR(20), "terminatingChannel" VARCHAR(100),
                "terminatingChannelName" VARCHAR(255), "recipientAccountNumber" VARCHAR(50),
                "recipientMobileNumber" VARCHAR(20), currency VARCHAR(20),
                "orgAmount" NUMERIC(31, 2), "usdAmount" NUMERIC(31, 2), "tzsAmount" NUMERIC(31, 2),
                "agentPrincipal" VARCHAR(50)
            )
        ''')
        cursor.execute('CREATE UNIQUE INDEX IF NOT EXISTS idx_outgoingRemittances_record_key ON "outgoingRemittances" ("recordKey")')
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
            'outflow', r['remittance_type'], r['remittance_sub_type'],
            r['sender_name'], r['sender_region'], r['sender_district'], r['sender_ward'],
            r['sender_street'], r['sender_house_number'], r['sender_postal_code'], 'TZ',
            r['sender_identification_type'], r['sender_identification_number'],
            r['mobile_number'], r['transfer_channel'], r['sub_category_transfer_channel'],
            r['recipient_name'], r['recipient_country'], r['recipient_postal_code'], r['recipient_house_number'],
            r['terminating_channel'], r['terminating_channel_name'],
            r['recipient_account_number'], r['recipient_mobile_number'],
            currency, org_amount, usd_amount, tzs_amount, r['agent_principal'],
        ))
    return rows


def replace_table(rows):
    setup_target_table()
    with postgres_connection() as connection, connection.cursor() as cursor:
        cursor.execute('DELETE FROM "outgoingRemittances"')
        psycopg2.extras.execute_values(cursor, '''
            INSERT INTO "outgoingRemittances" (
                "recordKey", "reportingDate", "transactionId", "transactionDate",
                "remittanceTransactionType", "remittanceType", "remittanceSubType",
                "senderName", "senderRegion", "senderDistrict", "senderWard",
                "senderStreet", "senderHouseNumber", "senderPostalCode", "senderCountry",
                "senderIdentificationType", "senderIdentificationNumber",
                "mobileNumber", "transferChannel", "subCategoryTransferChannel",
                "recipientName", "recipientCountry", "recipientPostalCode", "recipientHouseNumber",
                "terminatingChannel", "terminatingChannelName",
                "recipientAccountNumber", "recipientMobileNumber",
                currency, "orgAmount", "usdAmount", "tzsAmount", "agentPrincipal"
            ) VALUES %s
        ''', rows)
        connection.commit()


if __name__ == '__main__':
    logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
    seeded_rows = build_rows()
    replace_table(seeded_rows)
    logger.info('outgoingRemittances replaced with %s dummy row(s).', len(seeded_rows))
