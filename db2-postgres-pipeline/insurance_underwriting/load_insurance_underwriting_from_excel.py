#!/usr/bin/env python3
"""Load insuranceUnderwriting from the Bancassurance business unit's Excel file.

Same situation as insurance-commission and insurance-claim: PROFITS has no
bancassurance underwriting data, so this report is sourced entirely from the
business's own spreadsheet ("Insuarance Underwriting" sheet), never DB2.

Confirmed directly against the sheet (67 rows, 30 columns) before building:
  - status Of Insurer is "General Insurance" for all 67 rows - the insurance
    TYPE (duplicates "type Of Insurance business"), not a real D64
    active/inactive value. Not used for insurerStatus.
  - related Party and relationship Category are NULL in all 67 of 67 rows.
  - The sheet has no column at all for premiumRate or sectorSnaClassification.
  - "customer Identification Number" is literally a copy of "policy Number"
    for all 67 of 67 rows (e.g. both "RN-2304") - the business never
    populated a real customer identifier (no National ID, no bank CUST_ID)
    for these policies, so there's nothing reliable to cross-check against
    personalDataInformation/personalDataCorporates. Submitted as NULL
    (column is nullable) rather than the misleading policy-number
    duplicate, per business direction, until the business supplies a real
    identifier for these policyholders.

classBusiness/subClassBusiness01/subClassBusiness02 are mapped against the
BoT "General Insurance Business Class" lookup (confirmed with the business),
not passed through as the sheet's raw free text:
    classBusiness: "Motor Insurance"->"Motor", "Fire Insurance"->"Fire"
    subClassBusiness01: sheet's label -> the lookup's Sub-classes01 item
        (e.g. "2 Wheelers/ 3 Wheelers" -> "Motorcycle/3 Wheeler",
        "Vehicle/FLEET" -> "Motor Commercial" per business direction)
    subClassBusiness02: for Fire, mirrors subClassBusiness01 (the lookup
        defines no second level under Fire). For Motor, the coverage type
        (Comprehensive/Third Party Fire & Theft/Third Party Only) is read
        from the sheet's own text, but only trusted when that text doesn't
        contradict the row's own subClassBusiness01 - 2 rows in the source
        sheet had a subClass02 label copy-pasted from a different vehicle
        category (one said "Fire Domestic Package", another
        "Motorcycle/3 Wheeler - Comprehensive", both on "Private Vehicles"
        rows) and 8 rows had no subClass02 at all; all 10 fall back to
        'Third Party Only', the statutory-minimum/safest Motor cover,
        per business direction, rather than a fabricated specific plan.

Per business direction, these are populated with the most defensible
schema-valid value rather than left blank:
    insurerStatus          = 'active'                  (D64)
    premiumRate             = 0                          (no source)
    relatedParty            = 'Unrelated'                (D55 - plausible
                                                           default for retail
                                                           bancassurance
                                                           customers)
    relationshipCategory    = 'Direct'                   (D95 only offers
                                                           Direct/Indirect,
                                                           no neutral option -
                                                           this one is a
                                                           genuine unconfirmed
                                                           guess)
    sectorSnaClassification = derived from clientType:
                                 Individual -> 'Households'
                                 Corporate  -> 'Other Non-Financial Corporations'

Other lookups applied from the real sheet values (Annexure 5):
    insuranceBusinessType (D125): 'General Insurance' -> 'General'
    clientType (D54): 'Individual' -> 'Individual', 'Corporate' -> 'Corporates'
    gender (D93): 'M' -> 'Male', 'F' -> 'Female', blank -> 'Not Applicable'
    disability (D94): 'NIL'/blank -> 'None' (D94 item 1's literal Item
                       value - confirmed with keep_default_na=False; a
                       plain pd.read_excel() misreads that cell as NaN
                       because "None" is one of pandas' own default
                       null-sentinel strings)
    frequencyPremium (D09): sheet's numeric code mapped to its D09 label
                             (e.g. 1 -> 'Bullet')

Each run of this script REPLACES the entire contents of "insuranceUnderwriting"
with exactly what is in the given spreadsheet, same as the other two loaders.

Usage:
    python load_insurance_underwriting_from_excel.py "<path to xlsx>"
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

SHEET_NAME = 'Insuarance Underwriting'

D09_FREQUENCY = {
    1: 'Bullet', 2: '14 days', 3: '15 days', 4: '30 days', 5: '60 days',
    6: '90 days', 7: '120 days', 8: '150 days', 9: '180 days', 10: '360 days',
    11: 'Irregular',
}

# classBusiness / subClassBusiness01 / subClassBusiness02 -- the BoT
# "General Insurance Business Class" lookup (image supplied by the user).
# The sheet's free-text labels were checked against it row by row; only
# Fire and Motor appear in the 67-row sheet.
FIRE_SUBCLASS01 = {
    'domestic package.': 'Fire Domestic',
    'fire and allied perils fire class i': 'Fire Industrial',
}
MOTOR_SUBCLASS01 = {
    '2 wheelers/ 3 wheelers': 'Motorcycle/3 Wheeler',
    'commercial passenger carrying': 'Passenger Vehicle',
    'commercial vehicles': 'Motor Commercial',
    'private vehicles': 'Motor Private',
    'private motor insurance': 'Motor Private',  # confirmed with business: same category as 'private vehicles'
    'vehicle/fleet': 'Motor Commercial',  # confirmed with business: fleet vehicles are commercial
}

logger = logging.getLogger(__name__)


def _text_or_unknown(value):
    if pd.isna(value):
        return 'Unknown'
    text = str(value).strip()
    return text or 'Unknown'


def _format_date(value):
    return pd.Timestamp(value).strftime('%d%m%Y%H%M')


def _map_insurance_business_type(value):
    text = str(value).strip().lower()
    if 'life' in text:
        return 'Life'
    if 'general' in text:
        return 'General'
    return 'General'  # no other values observed in the source; General is the default


def _map_client_type(value):
    text = str(value).strip().lower()
    if text == 'corporate':
        return 'Corporates'
    return 'Individual'  # covers 'Individual' and any unrecognized value


def _map_disability(value):
    # D94 item 1 is literally the text "None" - confirmed by re-reading the
    # lookup workbook with keep_default_na=False. A plain pd.read_excel()
    # call silently turns that cell into NaN because "None" is one of
    # pandas' own default null-sentinel strings, which is why an earlier
    # pass at this mapping wrongly concluded the item was blank.
    if pd.isna(value):
        return 'None'
    text = str(value).strip()
    if text.upper() == 'NIL':
        return 'None'
    return text


def _map_gender(value):
    if pd.isna(value):
        return 'Not Applicable'
    text = str(value).strip().upper()
    if text == 'M':
        return 'Male'
    if text == 'F':
        return 'Female'
    return 'Not Applicable'


def _map_frequency_premium(value):
    try:
        return D09_FREQUENCY[int(value)]
    except (KeyError, ValueError, TypeError):
        return 'Irregular'  # fallback for any code not in the D09 table


def _map_sector_sna(client_type_raw):
    text = str(client_type_raw).strip().lower()
    if text == 'corporate':
        return 'Other Non-Financial Corporations'
    return 'Households'


def _normalize(text):
    return ' '.join(str(text).split()).strip().lower()


def _map_class_business(value):
    text = _normalize(value)
    if 'fire' in text:
        return 'Fire'
    if 'motor' in text:
        return 'Motor'
    raise ValueError(f'Unrecognized "class Of Business" value: {value!r} - add it to the lookup.')


def _map_sub_class01(class_business, raw_value):
    table = FIRE_SUBCLASS01 if class_business == 'Fire' else MOTOR_SUBCLASS01
    mapped = table.get(_normalize(raw_value))
    if mapped is None:
        raise ValueError(
            f'Unrecognized "subBusiness Class01" value {raw_value!r} for classBusiness '
            f'{class_business!r} - add it to the lookup.'
        )
    return mapped


def _map_sub_class02(class_business, mapped_subclass01, raw_value):
    # The BoT lookup has no second sub-class level under Fire (only
    # Fire Industrial/Fire Domestic at the subClass01 level) - mirror
    # subClass01, same as the sheet's own well-formed Fire rows already do.
    if class_business == 'Fire':
        return mapped_subclass01

    # Motor: extract the coverage type (Comprehensive / Third Party Fire &
    # Theft / Third Party Only) from the sheet's free text, but only trust
    # it when the same text's own vehicle-type wording doesn't contradict
    # this row's mapped subClass01 - guards against values copy-pasted from
    # a different vehicle category (confirmed in the source sheet: e.g. a
    # "Private Vehicles" row carrying "Fire Domestic Package" or a
    # "Motorcycle/3 Wheeler - Comprehensive" label). Blank or contaminated
    # text falls back to 'Third Party Only', the statutory-minimum/safest
    # Motor cover level, rather than fabricating a specific plan.
    text = _normalize(raw_value) if pd.notna(raw_value) else ''

    mentions_motorcycle = 'motorcycle' in text or 'wheeler' in text
    mentions_commercial = 'commercial' in text
    mentions_fire = 'fire' in text

    consistent = True
    if mapped_subclass01 == 'Motor Private' and (mentions_motorcycle or mentions_commercial or mentions_fire):
        consistent = False
    elif mapped_subclass01 == 'Motorcycle/3 Wheeler' and text and not mentions_motorcycle:
        consistent = False
    elif mapped_subclass01 == 'Motor Commercial' and text and not mentions_commercial:
        consistent = False

    if consistent:
        if 'third party fire' in text:
            return 'Third Party Fire & Theft'
        if 'third party' in text:
            return 'Third Party Only'
        if 'comprehensive' in text:
            return 'Comprehensive'
    return 'Third Party Only'


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
            CREATE TABLE IF NOT EXISTS "insuranceUnderwriting" (
                id BIGSERIAL PRIMARY KEY,
                "recordKey" VARCHAR(64) UNIQUE,
                "reportingDate" VARCHAR(12), "customerIdentificationNumber" VARCHAR(255),
                "bancassuranceId" VARCHAR(255), "policyNumber" VARCHAR(255), "clientName" VARCHAR(255),
                "insurerName" VARCHAR(255), "insurerStatus" VARCHAR(50), "clientCountry" VARCHAR(100),
                "insuranceBusinessType" VARCHAR(50), "classBusiness" VARCHAR(255),
                "subClassBusiness01" VARCHAR(255), "subClassBusiness02" VARCHAR(255),
                "insuranceProductName" VARCHAR(255), gender VARCHAR(20), disability VARCHAR(100),
                "clientType" VARCHAR(50), "relatedParty" VARCHAR(50), "relationshipCategory" VARCHAR(50),
                "branchCode" VARCHAR(20), region VARCHAR(100), district VARCHAR(100),
                "responsibleOfficer" VARCHAR(255), "responsibleSupervisor" VARCHAR(255), currency VARCHAR(20),
                "orgSumInsuredAmount" NUMERIC(31, 2), "tzsSumInsuredAmount" NUMERIC(31, 2),
                "premiumRate" NUMERIC(31, 6), "orgPremiumAmount" NUMERIC(31, 2), "tzsPremiumAmount" NUMERIC(31, 2),
                "frequencyPremium" VARCHAR(50), "commissionRate" NUMERIC(31, 6),
                "policyStartDate" VARCHAR(12), "policyMaturityDate" VARCHAR(12),
                "sectorSnaClassification" VARCHAR(100), "economicActivity" VARCHAR(255)
            )
        ''')
        cursor.execute('CREATE UNIQUE INDEX IF NOT EXISTS idx_insuranceUnderwriting_record_key ON "insuranceUnderwriting" ("recordKey")')
        connection.commit()


def load_rows(xlsx_path):
    df = pd.read_excel(xlsx_path, sheet_name=SHEET_NAME)
    df.columns = [str(c).strip() for c in df.columns]

    required = [
        'customer Identification Number', 'Bancassurance Identification', 'policy Number',
        'client Name', 'name of Insurer', 'country Of client', 'type Of Insurance business',
        'class Of Business', 'subBusiness Class01', 'subBusiness Class02', 'insurance Product Name',
        'gender', 'disability', 'clientType', 'branch Code', 'region', 'district Name',
        'responsible Officer', 'responsible Supervisor', 'currency', 'original Amount Sum Insured',
        'Original Amount Premium Received', 'frequency of premium', 'commission Rate (%)',
        'policy Start Date', 'policy Maturity Date', 'economic Activity',
    ]
    missing = [c for c in required if c not in df.columns]
    if missing:
        raise ValueError(f'Expected column(s) not found in "{SHEET_NAME}" sheet: {missing}')

    report_timestamp = pd.Timestamp.now().strftime('%d%m%Y%H%M')
    rows = []
    for i, r in df.iterrows():
        policy_number = _text_or_unknown(r['policy Number'])
        currency = str(r['currency']).strip().upper()
        org_sum_insured = float(r['original Amount Sum Insured'])
        org_premium = float(r['Original Amount Premium Received'])
        client_type_raw = r['clientType']

        class_business = _map_class_business(r['class Of Business'])
        sub_class01 = _map_sub_class01(class_business, r['subBusiness Class01'])
        sub_class02 = _map_sub_class02(class_business, sub_class01, r['subBusiness Class02'])

        if currency != 'TZS':
            raise ValueError(
                f'Row {i} (policy {policy_number}) is in {currency}, not TZS - '
                'FX conversion to tzsSumInsuredAmount/tzsPremiumAmount is not implemented yet.'
            )
        tzs_sum_insured = org_sum_insured
        tzs_premium = org_premium

        record_key = hashlib.sha256(policy_number.encode('utf-8')).hexdigest()

        rows.append((
            record_key, report_timestamp,
            None,  # customerIdentificationNumber -- nullable; see module docstring
            _text_or_unknown(r['Bancassurance Identification']),
            policy_number,
            _text_or_unknown(r['client Name']),
            _text_or_unknown(r['name of Insurer']),
            'active',  # insurerStatus -- no real source, see module docstring
            _text_or_unknown(r['country Of client']),
            _map_insurance_business_type(r['type Of Insurance business']),
            class_business,
            sub_class01,
            sub_class02,
            _text_or_unknown(r['insurance Product Name']),
            _map_gender(r['gender']),
            _map_disability(r['disability']),
            _map_client_type(client_type_raw),
            'Unrelated',  # relatedParty -- no source, see module docstring
            'Direct',     # relationshipCategory -- no source, unconfirmed guess
            _text_or_unknown(r['branch Code']),
            _text_or_unknown(r['region']),
            _text_or_unknown(r['district Name']),
            _text_or_unknown(r['responsible Officer']),
            _text_or_unknown(r['responsible Supervisor']),
            currency,
            org_sum_insured, tzs_sum_insured,
            0,  # premiumRate -- no source, see module docstring
            org_premium, tzs_premium,
            _map_frequency_premium(r['frequency of premium']),
            float(r['commission Rate (%)']),
            _format_date(r['policy Start Date']),
            _format_date(r['policy Maturity Date']),
            _map_sector_sna(client_type_raw),  # sectorSnaClassification -- derived, no direct source
            _text_or_unknown(r['economic Activity']),
        ))
    return rows


def replace_table(rows):
    setup_target_table()
    with postgres_connection() as connection, connection.cursor() as cursor:
        cursor.execute('DELETE FROM "insuranceUnderwriting"')
        psycopg2.extras.execute_values(cursor, '''
            INSERT INTO "insuranceUnderwriting" (
                "recordKey", "reportingDate", "customerIdentificationNumber", "bancassuranceId",
                "policyNumber", "clientName", "insurerName", "insurerStatus", "clientCountry",
                "insuranceBusinessType", "classBusiness", "subClassBusiness01", "subClassBusiness02",
                "insuranceProductName", gender, disability, "clientType", "relatedParty",
                "relationshipCategory", "branchCode", region, district, "responsibleOfficer",
                "responsibleSupervisor", currency, "orgSumInsuredAmount", "tzsSumInsuredAmount",
                "premiumRate", "orgPremiumAmount", "tzsPremiumAmount", "frequencyPremium",
                "commissionRate", "policyStartDate", "policyMaturityDate", "sectorSnaClassification",
                "economicActivity"
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
    logger.info('insuranceUnderwriting replaced from "%s": %s row(s) loaded.', sys.argv[1], len(loaded_rows))
