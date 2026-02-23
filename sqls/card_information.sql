SELECT
    VARCHAR_FORMAT(CURRENT_TIMESTAMP,'DDMMYYYYHHMM') AS reportingDate,
    'MWCOTZTZ' AS bankCode,
    CA.FULL_CARD_NO AS cardNumber,
    RIGHT(TRIM(CA.FULL_CARD_NO), 10) AS binNumber,
    CA.FK_CUST_ID AS customerIdentificationNumber,
    'Debit' AS cardType,
    NULL AS cardTypeSubCategory,
    VARCHAR_FORMAT(CA.TUN_DATE,'DDMMYYYYHHMM') AS cardIssueDate,
    'Mwalimu Commercial Bank Plc' AS cardIssuer,
    'Domestic' AS cardIssuerCategory,
    'TANZANIA, UNITED REPUBLIC OF' AS cardIssuerCountry,
    CA.CARD_NAME_LATIN AS cardHolderName,
    CASE
        WHEN CURRENT_DATE > CA.CARD_EXPIRY_DATE then 'Active'
        ELSE 'Inactive'
    END AS cardStatus,
    'VISA' AS cardScheme,
    'UBX Tanzania Limited' AS acquiringPartner,
    VARCHAR_FORMAT(CA.CARD_EXPIRY_DATE,'DDMMYYYYHHMM') AS cardExpireDate

FROM
    CMS_CARD CA