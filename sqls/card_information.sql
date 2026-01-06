SELECT
    CURRENT_TIMESTAMP AS reportingDate,
    ca.TUN_UNIT AS bankCode,
    CA.FULL_CARD_NO AS cardNumber,
    RIGHT(TRIM(CA.FULL_CARD_NO), 10) AS binNumber,
    CA.FK_CUST_ID AS customerIdentificationNumber,
    'Debit' AS cardType,  
    NULL AS cardTypeSubCategory,  
    CA.TUN_DATE AS cardIssueDate, 
    'Mwalimu Commercial Bank' AS cardIssuer,  
    'Domestic' AS cardIssuerCategory,  
    'Tanzania' AS cardIssuerCountry,  
    CA.CARD_NAME_LATIN AS cardHolderName,  
    CASE
        WHEN CURRENT_DATE > CA.CARD_EXPIRY_DATE then 'Active'
        ELSE 'Inactive'
    END AS cardStatus, 
    'VISA' AS cardScheme,  
    'UBX Tanzania Limited' AS acquiringPartner,  
    CA.CARD_EXPIRY_DATE AS cardExpireDate  

FROM
    CMS_CARD CA