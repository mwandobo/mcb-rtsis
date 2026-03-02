select gte.*,
       'separtator' as separator,
       la.*

from GLI_TRX_EXTRACT as gte
         --          LEFT JOIN (
--     SELECT la.*
--     FROM LOAN_ACCOUNT la
--              INNER JOIN (
--         SELECT
--             CUST_ID,
--             FK_LOANFK_PRODUCTI,
--             FK_AGREEMENTAGR_SN,
--             MAX(TMSTAMP) AS MAX_TS
--         FROM LOAN_ACCOUNT
--         GROUP BY
--             CUST_ID,
--             FK_LOANFK_PRODUCTI,
--             FK_AGREEMENTAGR_SN
--     ) x
--                         ON la.CUST_ID = x.CUST_ID
--                             AND la.FK_LOANFK_PRODUCTI = x.FK_LOANFK_PRODUCTI
--                             AND la.FK_AGREEMENTAGR_SN = x.FK_AGREEMENTAGR_SN
--                             AND la.TMSTAMP = x.MAX_TS
-- ) la
--                    ON gte.ID_PRODUCT = la.FK_LOANFK_PRODUCTI
--                        AND gte.CUST_ID   = la.CUST_ID

         LEFT JOIN LOAN_ACCOUNT la
                   ON gte.ID_PRODUCT = la.FK_LOANFK_PRODUCTI
                       AND la.CUST_ID = gte.CUST_ID


--          LEFT JOIN LOAN_ACCOUNT la
--                    ON gte.ID_PRODUCT = la.FK_LOANFK_PRODUCTI
--                        AND la.CUST_ID     = gte.CUST_ID
--                        AND la.AGREEMENT_SN = gte.BILL_SERIAL_NUM
--                        AND TRIM(CHAR(la.FK_UNITCODE))
--                           = TRIM(CHAR(gte.FK_UNITCODETRXUNIT))


--          LEFT JOIN LOAN_ACCOUNT la
--                    ON gte.ID_PRODUCT = la.FK_LOANFK_PRODUCTI
--                        and la.CUST_ID = gte.CUST_ID
-- and        la.AGREEMENT_SN = gte.BILL_SERIAL_NUM
--                        AND TRIM(CHAR(la.FK_UNITCODE)) = TRIM(CHAR(gte.FK_UNITCODETRXUNIT))
--          LEFT JOIN CUSTOMER as c ON la.CUST_ID = c.CUST_ID
--
--          LEFT JOIN other_id id ON (CASE WHEN (id.serial_no IS NULL) THEN '1' ELSE id.main_flag END = '1' AND
--                                    id.fk_customercust_id = c.cust_id)
-- --
--          LEFT JOIN generic_detail id_country ON (id.fkgh_has_been_issu = id_country.fk_generic_headpar AND
--                                                  id.fkgd_has_been_issu = id_country.serial_num)
--          LEFT JOIN COUNTRIES_LOOKUP cl ON cl.COUNTRY_NAME = id_country.description
WHERE gte.FK_GLG_ACCOUNTACCO IN ('7.0.5.19.0001', '7.0.5.19.0002', '7.0.5.19.0003')
;



SELECT gte.*,
       'separator' AS separator,
       la.*
FROM GLI_TRX_EXTRACT AS gte
         LEFT JOIN (SELECT la.*,
                           ROW_NUMBER() OVER (
                               PARTITION BY la.CUST_ID, la.FK_LOANFK_PRODUCTI
                               ORDER BY la.TMSTAMP DESC, la.ACC_OPEN_DT DESC
                               ) AS rn
                    FROM LOAN_ACCOUNT la) la
                   ON gte.ID_PRODUCT = la.FK_LOANFK_PRODUCTI
                       AND la.CUST_ID = gte.CUST_ID
                       AND la.rn = 1 -- Only the most recent loan
WHERE gte.FK_GLG_ACCOUNTACCO IN ('7.0.5.19.0001', '7.0.5.19.0002', '7.0.5.19.0003');


SELECT gte.*,
       'separator' AS separator,
       la.*
FROM GLI_TRX_EXTRACT AS gte
         INNER JOIN (SELECT la.*,
                            ROW_NUMBER() OVER (
                                PARTITION BY la.CUST_ID, la.FK_LOANFK_PRODUCTI
                                ORDER BY la.TMSTAMP DESC, la.ACC_OPEN_DT DESC
                                ) AS rn
                     FROM LOAN_ACCOUNT la) la
                    ON gte.ID_PRODUCT = la.FK_LOANFK_PRODUCTI
                        AND la.CUST_ID = gte.CUST_ID
                        AND la.rn = 1 -- Only the most recent loan
WHERE gte.FK_GLG_ACCOUNTACCO IN ('7.0.5.19.0001', '7.0.5.19.0002', '7.0.5.19.0003');


