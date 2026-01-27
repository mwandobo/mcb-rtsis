#!/usr/bin/env python3
"""
Compare INSERT fields with table structure
"""

import psycopg2
from config import Config
import re

# Get table fields
config = Config()
pg_conn = psycopg2.connect(
    host=config.database.pg_host,
    port=config.database.pg_port,
    database=config.database.pg_database,
    user=config.database.pg_user,
    password=config.database.pg_password
)

pg_cursor = pg_conn.cursor()
pg_cursor.execute("""
    SELECT column_name 
    FROM information_schema.columns 
    WHERE table_name = 'personalDataCorporate' 
    AND column_name NOT IN ('id', 'created_at', 'updated_at')
    ORDER BY ordinal_position
""")

table_columns = [row[0] for row in pg_cursor.fetchall()]
pg_cursor.close()
pg_conn.close()

# Get INSERT fields
insert_query = """
    "reportingDate", "companyName", "customerIdentificationNumber", "establishmentDate", "legalForm",
    "negativeClientStatus", "numberOfEmployees", "numberOfEmployeesMale", "numberOfEmployeesFemale",
    "registrationCountry", "registrationNumber", "taxIdentificationNumber", "tradeName", "parentName",
    "parentIncorporationNumber", "groupId", "sectorSnaClassification", "fullName", "gender", "cellPhone",
    "relationType", "nationalId", "appointmentDate", "terminationDate", "rateValueOfSharesOwned",
    "amountValueOfSharesOwned", "street", "country", "region", "district", "ward", "houseNumber",
    "postalCode", "poBox", "zipCode", "primaryPostalCode", "primaryRegion", "primaryDistrict",
    "primaryWard", "primaryStreet", "primaryHouseNumber", "secondaryStreet", "secondaryHouseNumber",
    "secondaryPostalCode", "secondaryRegion", "secondaryDistrict", "secondaryCountry", "secondaryTextAddress",
    "mobileNumber", "alternativeMobileNumber", "fixedLineNumber", "faxNumber", "emailAddress", "socialMedia",
    "entityName", "entityType", "certificateIncorporation", "entityRegion", "entityDistrict", "entityWard",
    "entityStreet", "entityHouseNumber", "entityPostalCode", "groupParentCode", "shareOwnedPercentage",
    "shareOwnedAmount"
"""

insert_fields = re.findall(r'"(\w+)"', insert_query)

print(f"Table has {len(table_columns)} columns")
print(f"INSERT has {len(insert_fields)} fields")

# Find differences
table_set = set(table_columns)
insert_set = set(insert_fields)

extra_in_insert = insert_set - table_set
missing_in_insert = table_set - insert_set

if extra_in_insert:
    print(f"\nExtra fields in INSERT (not in table): {extra_in_insert}")

if missing_in_insert:
    print(f"\nMissing fields in INSERT (in table but not INSERT): {missing_in_insert}")

if not extra_in_insert and not missing_in_insert:
    print("\n✅ All fields match!")
else:
    print("\n❌ Field mismatch found!")
    
# Show side by side comparison
print(f"\nSide by side comparison:")
print(f"{'Table Column':<35} {'INSERT Field':<35}")
print("-" * 70)

max_len = max(len(table_columns), len(insert_fields))
for i in range(max_len):
    table_col = table_columns[i] if i < len(table_columns) else ""
    insert_col = insert_fields[i] if i < len(insert_fields) else ""
    match = "✅" if table_col == insert_col else "❌"
    print(f"{table_col:<35} {insert_col:<35} {match}")