SELECT VARCHAR_FORMAT(CURRENT_TIMESTAMP, 'DDMMYYYYHH24MI') AS reportingDate,
       LTRIM(RTRIM(W.ACCOUNT_NUMBER))                      AS accountNumber,
       LTRIM(RTRIM(id.ID_NO))                              AS customerIdentificationNumber,
       RTRIM(LTRIM(
               COALESCE(c.FIRST_NAME, '') || ' ' ||
               COALESCE(c.MIDDLE_NAME, '') || ' ' ||
               COALESCE(c.SURNAME, '')
             )
       )                                                   AS clientName,
       ctl.CUSTOMER_TYPE                                   as clientType,
       cl.COUNTRY_CODE                                     as borrowerCountry,
       null                                                as ratingStatus,
       null                                                as crRatingBorrower,
       null                                                as gradesUnratedBanks,
       null                                                as groupCode,
       0                                                   AS allowanceProbableLoss,
       0                                                   AS botProvision
FROM W_EOM_ACCOUNT W
         LEFT JOIN CUSTOMER c
                   ON c.CUST_ID = W.CUST_ID
         LEFT JOIN other_id id ON (CASE WHEN (id.serial_no IS NULL) THEN '1' ELSE id.main_flag END = '1' AND
                                   id.fk_customercust_id = c.cust_id)
         LEFT JOIN cust_address ca
                   ON (ca.fk_customercust_id = c.cust_id AND ca.communication_addr = '1' AND
                       ca.entry_status = '1')
             LEFT JOIN generic_detail id_country ON (id.fkgh_has_been_issu = id_country.fk_generic_headpar AND
                                                 id.fkgd_has_been_issu = id_country.serial_num)
         LEFT JOIN CUSTOMER_TYPES_LOOKUP ctl ON ctl.CUSTOMER_TYPE_CODE = c.CUST_TYPE
         LEFT JOIN COUNTRIES_LOOKUP cl ON cl.COUNTRY_NAME = id_country.description
    FETCH FIRST 100 ROWS ONLY;