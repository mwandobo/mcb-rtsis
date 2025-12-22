# claimTreasury Lookup Values Reference

## D32: Asset Classification Category

| Code | Value | Description | Past Due Days | IFRS Provision | BOT Provision |
|------|-------|-------------|---------------|----------------|---------------|
| 1 | Current | Performing assets | 0-30 days | 1% | 2% |
| 2 | EspeciallyMentioned | Watch/Special mention | 31-90 days | 5% | 5% |
| 3 | Substandard | Below standard | 91-180 days | 25% | 25% |
| 4 | Doubtful | Questionable recovery | 181-365 days | 50% | 50% |
| 5 | Loss | Uncollectible | Over 365 days | 100% | 100% |

## SNA: Sector Classification

| S/No | Sector SNA Classification | Description |
|------|---------------------------|-------------|
| 1 | Central Bank | Bank of Tanzania |
| 2 | General government | Other government entities |
| 3 | Central Governments | Ministries, Treasury, Central Government Agencies |
| 4 | Local Governments | Municipal Councils, District Councils, Local Authorities |
| 5 | Social security | Social security funds |
| 6 | Public Non-Financial Corporations | Government-owned enterprises, parastatals |
| 7 | Other Depository Corporations | Commercial banks, savings banks |
| 8 | Other financial Corporations | Financial institutions |
| 9 | Insurance Companies | Insurance providers |
| 10 | Pension Funds | Pension fund managers |
| 11 | Other Financial Intermediary | Other financial intermediaries |
| 12 | Other Non-Financial Corporations | Private corporations |
| 13 | Households | Individual customers |
| 14 | Nonprofit Institutions Serving Households | NGOs, charities |
| 15 | Nonresidents | Foreign entities |

## Currency Codes

Common currencies used in the system:
- **TZS**: Tanzanian Shilling (base currency)
- **USD**: United States Dollar
- **EUR**: Euro
- **GBP**: British Pound
- **KES**: Kenyan Shilling
- **UGX**: Ugandan Shilling

## Exchange Rate Reference

Current implementation uses:
- **TZS/USD Rate**: 2,500 (hardcoded, should be updated based on actual rates)

## Government Institution Keywords

The query identifies government institutions using these patterns:
- MINISTRY
- TREASURY
- GOVERNMENT
- COUNCIL
- MUNICIPAL
- DISTRICT
- AUTHORITY
- CORPORATION (for parastatals)
- PARASTATAL
- GOVT
- GOV

## Date Format

All dates must be formatted as: **DDMMYYYYHHMM**
- DD: Day (01-31)
- MM: Month (01-12)
- YYYY: Year (4 digits)
- HH: Hour (00-23)
- MM: Minute (00-59)

Example: 18122025143000 = December 18, 2025, 14:30:00

## Status Codes

TREASURY_MM_DEAL.STATUS values:
- **'1'**: Active deal
- Other values: Inactive/closed deals

TREASURY_MM_DEAL.MATURE_FLAG values:
- **'1'**: Matured/settled deal
- NULL or other: Not yet matured