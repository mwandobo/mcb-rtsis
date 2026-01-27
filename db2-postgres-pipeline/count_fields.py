#!/usr/bin/env python3
"""
Count fields and placeholders in the corporate processor
"""

query = """INSERT INTO "personalDataCorporate" (
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
)"""

import re
fields = re.findall(r'"(\w+)"', query)
print(f'Total fields: {len(fields)}')

placeholders = """%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,
%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,
%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,
%s, %s, %s, %s, %s, %s"""

placeholder_count = placeholders.count('%s')
print(f'Total placeholders: {placeholder_count}')

if len(fields) == placeholder_count:
    print("✅ Fields and placeholders match!")
else:
    print(f"❌ Mismatch: {len(fields)} fields vs {placeholder_count} placeholders")
    print(f"Need to {'add' if len(fields) > placeholder_count else 'remove'} {abs(len(fields) - placeholder_count)} placeholders")