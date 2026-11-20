# Complaint and Fraud Statistics (Bancassurance) Loader

PROFITS has no bancassurance complaint/fraud data, so this report is sourced
entirely from the Bancassurance business unit's own spreadsheet, never DB2 -
same pattern as `insurance_commission`, `insurance_claim`, and
`insurance_underwriting`.

## Source

The "complaints and fraud statistics" sheet in the original Bancassurance
workbook (`Bankassuarance (006).xlsx`) was empty. The only real record
available was recovered from a separate Excel crash-recovery file; its
`policy Number` cell was itself corrupted on recovery (read back as `0`).
Per business direction it is loaded as `RN-2303` instead.

`complaint_fraud_statistics_seed.xlsx` reproduces that one recovered row
(with the corrected policy number), using the same sheet name
(`complaints and fraud statistics`) and column headers as the original
workbook, so the loader also works unchanged against a future, fuller
version of that file - just point it at the new file instead of the seed.

`complaintNature` ("Repudiation of a claim"), `complaintStatus` ("referred"),
and `complaintsReferred` ("Employer") were checked against Annexure 5
(D127, D107, D57 respectively) - the recovered row's values are already
exact, valid lookup labels.

## Usage

```bash
python load_complaint_fraud_statistics_from_excel.py "<path to xlsx>"
```

Each run **replaces** the entire contents of `"complaintFraudStatistics"`
with exactly what is in the given spreadsheet.
