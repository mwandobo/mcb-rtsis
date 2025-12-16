CREATE FUNCTION DEP_AMOUNT_USD (
    DATE_IN	DATE,
    REPORT_NAME_IN	VARCHAR(20),
    REPORT_SHEET_IN	VARCHAR(20),
    REPORT_CELL_IN	VARCHAR(20) )
  RETURNS DECIMAL(15, 2)
  SPECIFIC SQL160728151028415
  NOT SECURED
  LANGUAGE SQL
  NOT DETERMINISTIC
  EXTERNAL ACTION
  READS SQL DATA
  INHERIT SPECIAL REGISTERS
BEGIN

DECLARE result0 decimal(15,2);

for v1 as  
WITH config1
           AS (                
                    SELECT DISTINCT 
                        REPORT,
                        EXCEL_SHEET,
                        EXCEL_CELL,
                        SN,
                        PRODUCT_TYPE_FROM,
                        PRODUCT_TYPE_TO,
                        PRODUCT_FROM,
                        PRODUCT_TO,
                        DEP_CATEGORY_FROM,
                        DEP_CATEGORY_TO,
                        ACTIVITY_FROM,
                        ACTIVITY_TO,
                        CUST_CATEGORY_FROM,
                        CUST_CATEGORY_TO,
                        ACCOUNT_STS_FROM,
                        ACCOUNT_STS_TO,
                        AMT_TYPE,
                        CURRENCY_TYPE,
                        CURRENCY,
                        FACTOR1,
                        DATE_FROM,
                        DATE_TO,
                        RATE_FROM,
                        RATE_TO,
                        MONTH_DURATION_FROM,
                        MONTH_DURATION_TO,
                        OPENING_DATE_FROM,
                        OPENING_DATE_TO,
                        REGION_FROM,
                        REGION_TO,
                        AMOUNT_FROM,
                        AMOUNT_TO,
                        MATURED
               FROM   profits_deposits_configuration
               WHERE      report = report_name_in
                      AND excel_sheet = report_sheet_in
                      AND excel_cell = report_cell_in)
          ,oneacc
           AS (SELECT   DISTINCT
                        ROUND (
                           NVL (
                                 (
                                   CASE
                                      WHEN amt_type = 'GROSS AMOUNT' THEN   (BOOK_BALANCE +ACCR_CR_INTEREST)*    
                                                                             (CASE WHEN eom_deposits.currency = 'USD' THEN 1
                                                                             ELSE ( select distinct fixing_rate 
                                                                                    from eom_deposits 
                                                                                    where eom_date = date_in 
                                                                                    and eom_deposits.currency = 'USD') * factor1 
                                                                             END)
                                      WHEN amt_type = 'FINAL INTEREST'      THEN (RATE)*1000
                                      WHEN amt_type = 'FINAL INTEREST AMT' THEN (ACCR_CR_INTEREST)*    
                                                                             (CASE WHEN eom_deposits.currency = 'USD' THEN 1
                                                                             ELSE ( select distinct fixing_rate 
                                                                                    from eom_deposits 
                                                                                    where eom_date = date_in 
                                                                                    and eom_deposits.currency = 'USD') * factor1 
                                                                             END)
                                      WHEN amt_type = 'NO OF ACCOUNTS'      THEN count(*)*1000                                           
                                      WHEN amt_type = 'BOOK BALANCE'        THEN (BOOK_BALANCE )*    
                                                                             (CASE WHEN eom_deposits.currency = 'USD' THEN 1
                                                                             ELSE ( select distinct fixing_rate 
                                                                                    from eom_deposits 
                                                                                    where eom_date = date_in 
                                                                                    and eom_deposits.currency = 'USD') * factor1 
                                                                             END)
                                      WHEN amt_type = 'INTEREST PAYABLE'    THEN (ACCR_CR_INTEREST)*    
                                                                             (CASE WHEN eom_deposits.currency = 'USD' THEN 1
                                                                             ELSE ( select distinct fixing_rate 
                                                                                    from eom_deposits 
                                                                                    where eom_date = date_in 
                                                                                    and eom_deposits.currency = 'USD') * factor1 
                                                                             END)
--                                      WHEN amt_type = 'RATE MIN'            THEN MIN(RATE)*1000
--                                      WHEN amt_type = 'RATE MAX'            THEN MAX(RATE)*1000                                         
                                      WHEN amt_type = 'WEIGHTED AVERAGE'    THEN  (
                                                                                    CASE WHEN BOOK_BALANCE <> 0 THEN
                                                                                        SUM (
                                                                                            (BOOK_BALANCE +ACCR_CR_INTEREST) *(
                                                                                                                                CASE WHEN eom_deposits.currency = 'USD' THEN 1 
                                                                                                                                ELSE (select distinct fixing_rate from eom_deposits where eom_date = date_in and eom_deposits.currency = 'USD')
                                                                                                                                END)* RATE* factor1
                                                                                                                              )/
                                                                                           SUM (
                                                                                            (BOOK_BALANCE +ACCR_CR_INTEREST) *(
                                                                                                                                CASE WHEN eom_deposits.currency = 'USD' THEN 1 
                                                                                                                                ELSE (select distinct fixing_rate from eom_deposits where eom_date = date_in and eom_deposits.currency = 'USD')
                                                                                                                                END)* factor1
                                                                                                                              )*1000
                                                                                    ELSE   sum( BOOK_BALANCE*(CASE WHEN eom_deposits.currency = 'USD' THEN 1 ELSE (select distinct fixing_rate from eom_deposits where eom_date = date_in and eom_deposits.currency = 'USD')END))*1000
                                                                                    END
                                                                                   )                                                                                    
                                   END)
                              / 1000
                             ,0)
                          ,0)
                           AS amount
FROM     eom_deposits, config1
               WHERE    
eom_deposits.entry_status NOT IN ('0', '3', '4')
                        AND eom_deposits.book_balance > 0
                        AND TRIM (eom_deposits.deposit_type)    BETWEEN TRIM (config1.product_type_from) AND TRIM (config1.product_type_to)
                        AND (eom_deposits.id_product)           BETWEEN TRIM (config1.product_from)AND TRIM (config1.product_to)
                        AND (eom_deposits.fkgd_type)            BETWEEN TRIM (config1.dep_category_from)AND TRIM (config1.dep_category_to)
                        AND (eom_deposits.fk_generic_detaser)   BETWEEN TRIM (config1.activity_from)AND TRIM (config1.activity_to)
                        AND (eom_deposits.fkgd_category)        BETWEEN TRIM (config1.cust_category_from)AND TRIM (config1.cust_category_to)
                        AND TRIM (eom_deposits.entry_status)    BETWEEN TRIM (config1.account_sts_from)AND TRIM (config1.account_sts_to)
                        AND (eom_deposits.RATE)                 BETWEEN config1.RATE_FROM AND config1.RATE_TO
                        AND MONTHS_BETWEEN (eom_deposits.expiry_date,eom_deposits.opening_date) BETWEEN (config1.month_duration_from) AND (config1.month_duration_to)
                        AND (eom_deposits.EURO_BOOK_BAL)        BETWEEN  (config1.AMOUNT_FROM)AND  (config1.AMOUNT_TO)
                        AND (eom_deposits.FKGD_RESIDES_IN_RE)   BETWEEN nvl(config1.REGION_FROM,0) AND nvl(config1.REGION_TO,99999)
--                        AND (
--                                (config1.currency = 'ALL') OR 
--                                (config1.currency) = 'FC'   AND eom_deposits.id_currency IN (SELECT id_currency FROM currency WHERE  national_flag <>'1')) or
--                                (config1.currency) = 'FC'  AND config1.CURRENCY_TYPE = 'USD' /* and eom_deposits.id_currency = 1*/ or
--                                (config1.currency = 'DC'  AND eom_deposits.id_currency IN (SELECT id_currency FROM currency WHERE  national_flag ='1')
--                            )
                        AND eom_deposits.id_product IN (SELECT par_relation_detai.fkgd_has_a_seconda
                                                        FROM   par_relation_detai
                                                        WHERE  eom_deposits.id_product =
                                                               par_relation_detai.fkgd_has_a_seconda AND 
                                                               par_relation_detai.fk_par_relationcod ='ONEACC')
                         and eom_date BETWEEN 
                        (CASE
                             WHEN config1.date_from = 'CURRENT'                     THEN  date_in       /* Normal */
                             WHEN config1.date_from = 'NOLIMIT'                     THEN  TO_DATE ('01/01/1970','DD/MM/YYYY')  
                             WHEN config1.date_from = 'FIRST DAY OF YEAR'           THEN (SELECT TRUNC ( date_in,'YEAR') FROM   DUAL) /* First Day of YEAR */
                        END)
                        AND 
                        (CASE
                             WHEN config1.date_to = 'CURRENT' THEN  date_in       /* Normal */
                             WHEN config1.date_to = 'First Day of YEAR'             THEN (SELECT TRUNC ( date_in,'YEAR') FROM   DUAL) /* First Day of YEAR */
                             WHEN config1.date_to = 'LAST DAY OF MONTH'             THEN (SELECT LAST_DAY ( date_in) FROM   DUAL) /* Last Day of MONTH */
                        END)                                                    
               GROUP BY 
               amt_type,
               BOOK_BALANCE,
               ACCR_CR_INTEREST,
               RATE,
               CURRENCY_TYPE,
               eom_deposits.currency,
               fixing_rate,
               factor1
               )
          ,oneacc0
           AS (SELECT amount FROM oneacc
               UNION ALL
               SELECT 0
               FROM   DUAL
               WHERE  NOT EXISTS (SELECT 0 FROM oneacc))
          ,acc
           AS (SELECT   DISTINCT
                         (
                           NVL (
                                 (
                                   CASE
                                      WHEN amt_type = 'GROSS AMOUNT' THEN   (BOOK_BALANCE +ACCR_CR_INTEREST)*    
                                                                             (CASE WHEN eom_deposits.currency = 'USD' THEN 1
                                                                             ELSE ( select distinct fixing_rate 
                                                                                    from eom_deposits 
                                                                                    where eom_date = date_in 
                                                                                    and eom_deposits.currency = 'USD') * factor1 
                                                                             END)
                                      WHEN amt_type = 'FINAL INTEREST'      THEN (RATE)*1000
                                      WHEN amt_type = 'FINAL INTEREST AMT' THEN (ACCR_CR_INTEREST)*    
                                                                             (CASE WHEN eom_deposits.currency = 'USD' THEN 1
                                                                             ELSE ( select distinct fixing_rate 
                                                                                    from eom_deposits 
                                                                                    where eom_date = date_in 
                                                                                    and eom_deposits.currency = 'USD') * factor1 
                                                                             END)
                                      WHEN amt_type = 'NO OF ACCOUNTS'      THEN count(*)*1000                                           
                                      WHEN amt_type = 'BOOK BALANCE'        THEN (BOOK_BALANCE )*    
                                                                             (CASE WHEN eom_deposits.currency = 'USD' THEN 1
                                                                             ELSE ( select distinct fixing_rate 
                                                                                    from eom_deposits 
                                                                                    where eom_date = date_in 
                                                                                    and eom_deposits.currency = 'USD') * factor1 
                                                                             END)
                                      WHEN amt_type = 'INTEREST PAYABLE'    THEN (ACCR_CR_INTEREST)*    
                                                                             (CASE WHEN eom_deposits.currency = 'USD' THEN 1
                                                                             ELSE ( select distinct fixing_rate 
                                                                                    from eom_deposits 
                                                                                    where eom_date = date_in 
                                                                                    and eom_deposits.currency = 'USD') * factor1 
                                                                             END)
--                                      WHEN amt_type = 'RATE MIN'            THEN MIN(RATE)*1000
--                                      WHEN amt_type = 'RATE MAX'            THEN MAX(RATE)*1000                                         
                                      WHEN amt_type = 'WEIGHTED AVERAGE'    THEN  (
                                                                                    CASE WHEN BOOK_BALANCE <> 0 THEN
                                                                                        sum (
                                                                                            (BOOK_BALANCE +ACCR_CR_INTEREST) *(
                                                                                                                                CASE WHEN eom_deposits.currency = 'USD' THEN 1 
                                                                                                                                ELSE (select distinct fixing_rate from eom_deposits where eom_date = date_in and eom_deposits.currency = 'USD')
                                                                                                                                END)* RATE* factor1
                                                                                                                              )/
                                                                                         sum   (
                                                                                            (BOOK_BALANCE +ACCR_CR_INTEREST) *(
                                                                                                                                CASE WHEN eom_deposits.currency = 'USD' THEN 1 
                                                                                                                                ELSE (select distinct fixing_rate from eom_deposits where eom_date = date_in and eom_deposits.currency = 'USD')
                                                                                                                                END)* factor1
                                                                                                                              )*1000
                                                                                    ELSE  sum ( BOOK_BALANCE*(CASE WHEN eom_deposits.currency = 'USD' THEN 1 ELSE (select distinct fixing_rate from eom_deposits where eom_date = date_in and eom_deposits.currency = 'USD')END))*1000
                                                                                    END
                                                                                   )                                                                                    
                                   END)
                              / 1000
                             ,0)
                          )
                           AS amount
               FROM     eom_deposits, config1
               WHERE        --eom_deposits.eom_date =  TO_DATE ('13/10/2017', 'DD/MM/YYYY')
                         eom_deposits.entry_status NOT IN ('0', '3', '4')
                        AND TRIM (eom_deposits.deposit_type)    BETWEEN TRIM (config1.product_type_from) AND TRIM (config1.product_type_to)
                        AND (eom_deposits.id_product)           BETWEEN TRIM (config1.product_from)AND TRIM (config1.product_to)
                        AND (eom_deposits.fkgd_type)            BETWEEN TRIM (config1.dep_category_from)AND TRIM (config1.dep_category_to)
                        AND (eom_deposits.fk_generic_detaser)   BETWEEN TRIM (config1.activity_from)AND TRIM (config1.activity_to)
                        AND (eom_deposits.fkgd_category)        BETWEEN TRIM (config1.cust_category_from)AND TRIM (config1.cust_category_to)
                        AND TRIM (eom_deposits.entry_status)    BETWEEN TRIM (config1.account_sts_from)AND TRIM (config1.account_sts_to)
                        AND (eom_deposits.RATE)                 BETWEEN config1.RATE_FROM AND config1.RATE_TO
                        AND MONTHS_BETWEEN (eom_deposits.expiry_date,eom_deposits.opening_date) BETWEEN (config1.month_duration_from) AND (config1.month_duration_to)
                        AND (eom_deposits.EURO_BOOK_BAL)        BETWEEN  (config1.AMOUNT_FROM)AND  (config1.AMOUNT_TO)
                        AND (eom_deposits.FKGD_RESIDES_IN_RE)   BETWEEN nvl(config1.REGION_FROM,0) AND nvl(config1.REGION_TO,99999)
--                        AND (
--                                (config1.currency = 'ALL') OR 
--                                (config1.currency = 'FC'   AND eom_deposits.id_currency IN (SELECT id_currency FROM currency WHERE  national_flag <>'1')) OR
--                                (config1.currency) = 'FC'  AND config1.CURRENCY_TYPE = 'USD' /* and eom_deposits.id_currency = 1*/ or
--                                (config1.currency = 'DC')  AND eom_deposits.id_currency IN (SELECT id_currency FROM currency WHERE  national_flag ='1')
--                            )
                        AND eom_deposits.id_product NOT IN (SELECT par_relation_detai.fkgd_has_a_seconda
                                                        FROM   par_relation_detai
                                                        WHERE  eom_deposits.id_product =
                                                               par_relation_detai.fkgd_has_a_seconda AND 
                                                               par_relation_detai.fk_par_relationcod ='ONEACC')
                         and eom_date BETWEEN 
                        (CASE
                             WHEN config1.date_from = 'CURRENT'                     THEN  date_in       /* Normal */
                             WHEN config1.date_from = 'NOLIMIT'                     THEN  TO_DATE ('01/01/1970','DD/MM/YYYY')  
                             WHEN config1.date_from = 'First Day of YEAR'           THEN (SELECT TRUNC ( date_in,'YEAR') FROM   DUAL) /* First Day of YEAR */
                        END)
                        AND 
                        (CASE
                             WHEN config1.date_to = 'CURRENT' THEN  date_in       /* Normal */
                             WHEN config1.date_to = 'FIRST DAY OF YEAR'             THEN (SELECT TRUNC ( date_in,'YEAR') FROM   DUAL) /* First Day of YEAR */
                             WHEN config1.date_to = 'LAST DAY OF MONTH'             THEN (SELECT LAST_DAY ( date_in) FROM   DUAL) /* Last Day of MONTH */
                        END)                                                                          
               GROUP BY 
--               config1.currency
amt_type,
               BOOK_BALANCE,
               ACCR_CR_INTEREST,
               RATE,
               CURRENCY_TYPE,
               eom_deposits.currency,
               fixing_rate,
               factor1
               )
          ,acc0
           AS (SELECT amount FROM acc
               UNION ALL
               SELECT 0
               FROM   DUAL
               WHERE  NOT EXISTS (SELECT 0 FROM acc))
      SELECT NVL (SUM (NVL (oneacc0.amount, 0) + NVL (acc0.amount, 0)), 0) AS amount
      FROM   oneacc0, acc0
DO
  SET result0 = v1.amount;
END FOR;

return result0;

END;

