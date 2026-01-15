SELECT CURRENT_TIMESTAMP                                              AS reportingDate,
       ca.FULL_CARD_NO                                                as cardNumber,
       LPAD(CHAR(ca.CARD_NUMBER), 10, '0')                            AS binNumber,
       'Mwalimu Commercial Bank Plc'                                  as transactingBankName,
       ce.ISO_REF_NUM                                                 as transactionId,
       ce.TUN_DATE                                                    as transactionDate,
       'Local Transactions by Locally Issued Cards'                   as transactionNature,
       null                                                           as atmCode,
       null                                                           as posNumber,
       pc.DESCRIPTION                                                 as transactionDescription,
       ca.CARD_NAME_LATIN                                             as beneficiaryName,
       null                                                           as beneficiaryTradeName,
       'TANZANIA, UNITED REPUBLIC OF'                                 as beneficaryCountry,
       'TANZANIA, UNITED REPUBLIC OF'                                 as transactionPlace,
       null                                                           as qtyItemsPurchased,
       null                                                           as unitPrice,
       null                                                           as orgFacilitatorCommissionAmount,
       null                                                           as usdFacilitatorCommissionAmount,
       null                                                           as tzsFacilitatorCommissionAmount,
       'TZS'                                                          as currency,
       ce.TRANSACTION_AMNT                                            as orgTransactionAmount,
       CAST(ROUND(ce.TRANSACTION_AMNT / 2500.0, 2) AS DECIMAL(15, 2)) AS usdTransactionAmount,
       ce.TRANSACTION_AMNT                                            as tzsTransactionAmount

FROM CMS_CARD_EXTRAIT ce
         LEFT JOIN CMS_CARD ca ON ca.CARD_SN = ce.CARD_SN
         LEFT JOIN ATM_PROCESS_CODE pc ON pc.ISO_CODE = ce.PROCESS_CD;
