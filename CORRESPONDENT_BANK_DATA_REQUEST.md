# Missing Mandatory Source Data for `personalDataCorporates`

The fields below are mandatory in the BOT Personal Data_Company specification and are absent, invalid, or not available from the current DB2 source for these two customers. Agreed hardcoded/default values used by the query are intentionally not listed.

| Customer ID | Institution | Account number |
|---:|---|---|
| `9999901` | BANK OF TANZANIA | `9999010002` |
| `9999902` | CITIBANK NEW YORK | `9999020001` |

## Missing or invalid mandatory fields

| `personalDataCorporates` field | `9999901` — BANK OF TANZANIA | `9999902` — CITIBANK NEW YORK |
|---|---|---|
| `establishedDate` | Invalid source value: `0001-01-01` | Invalid source value: `0001-01-01` |
| `numberOfEmployees` | Source value is `0`; require the actual employee count | Source value is `0`; require the actual employee count |
| `totalEmployeesMale` | Current query outputs `0`; require the actual male employee count | Current query outputs `0`; require the actual male employee count |
| `totalEmployeesFemale` | Current query outputs `0`; require the actual female employee count | Current query outputs `0`; require the actual female employee count |
| `registrationNumber` | No source `ID_NO` | No source `ID_NO` |
| `taxIdentificationNumber` | No source `ID_NO` | No source `ID_NO` |
| `certificateIncorporation` | No source `ID_NO` | No source `ID_NO` |
| `zipCode` | Empty in source | Empty in source |
| `region` | Empty in source | Available: `NEW YORK` |

## Mandatory related-person data

The current query derives `related_customers` through an `AGREEMENT` relationship. Both customers have no agreement/address match, so no related-person record can be produced.

Please provide, for each required related person:

- `fullName`
- `gender`
- `cellPhone`
- `relationType`
- `nationalId`
- `nationality`
- `appointmentDate`

## Available source values

| Customer ID | `companyName` / `tradeName` | `W_DIM_CUSTOMER.CUSTOMER_BEGIN_DAT` | Source address |
|---:|---|---|---|
| `9999901` | BANK OF TANZANIA | `2016-03-24` | MIRAMBO ST, DAR ES SALAAM |
| `9999902` | CITIBANK NEW YORK | `2016-03-24` | NEW YORK, NEW YORK |
