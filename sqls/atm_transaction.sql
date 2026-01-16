select
    CURRENT_TIMESTAMP as reportingDate,
    atx.TERMINAL as atmCode,
    atx.TUN_DATE as transactionDate,
    atx.REFERENCE_NUMBER as transactionId,
    CASE
        WHEN atx.PROCESSING_CODE IN ('010000','011000','011096','012000') THEN 'Cash Withdrawal'
        WHEN atx.PROCESSING_CODE IN ('001000','002000','311000','312000') THEN 'Account balance enquiries'
        WHEN atx.PROCESSING_CODE = '219610' THEN 'Reversal/Cancellation'
        ELSE NULL
    END as transactionType,
    'TZS' as currency,
    atx.TRANSACTION_AMOUNT as orgTransactionAmount,
    atx.TRANSACTION_AMOUNT as tzsTransactionAmount,
    'Card and Mobile Based' as atmChannel,
   DECIMAL(atx.TRANSACTION_AMOUNT * 0.18, 15, 2) AS valueAddedTaxAmount,
    0 as exciseDutyAmount,
    0 as electronicLevyAmount
FROM ATM_TRX_RECORDING atx
LEFT JOIN ATM_PROCESS_CODE pc ON pc.ISO_CODE = atx.PROCESSING_CODE WHERE TERMINAL in('MWL01001','MWL01002');