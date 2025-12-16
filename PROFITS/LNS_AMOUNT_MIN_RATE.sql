CREATE FUNCTION LNS_AMOUNT_MIN_RATE (
    DATE_IN	DATE,
    REPORT_NAME_IN	VARCHAR(20),
    REPORT_SHEET_IN	VARCHAR(20),
    REPORT_CELL_IN	VARCHAR(20) )
  RETURNS DECIMAL(15, 2)
  SPECIFIC SQL160728131623202
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
     AS (SELECT DISTINCT report
                        ,excel_sheet
                        ,excel_cell
                        ,lns_sn
                        ,product_from
                        ,product_to
                        ,finsc_from
                        ,finsc_to
                        ,ccode_from
                        ,ccode_to
                        ,lpurp_from
                        ,lpurp_to
                        ,lpucb_from
                        ,lpucb_to
                        ,loan_sts_from
                        ,loan_sts_to
                        ,classific_from
                        ,classific_to
                        ,amt_type
                        ,currency_type
                        ,currency
                        ,factor1
                        ,date_from
                        ,date_to
                        ,sub_class_from
                        ,sub_class_to
                        ,ov_days_from
                        ,ov_days_to
                        ,cust_categ_from
                        ,cust_categ_to
         FROM   profits_loans_configuration
               WHERE      report = report_name_in
                      AND excel_sheet = report_sheet_in
                      AND excel_cell = report_cell_in)
    ,loan0
     AS (SELECT t.acc_unit
               ,t.acc_type
               ,t.acc_sn
               ,p.account_number
               ,p.account_cd
               ,t.capital_db
               ,t.rl_int_db
               ,t.rl_pnl_int_db
               ,t.rl_int_tr
               ,t.capital_tr
               ,t.capital_cr
               ,t.rl_int_cr
               ,t.rl_pnl_int_cr
         FROM   loan_account_total t, profits_account p, loan_account l
         WHERE      t.acc_sn = p.lns_sn
                AND t.acc_type = p.lns_type
                AND t.acc_unit = p.lns_open_unit
                AND l.acc_sn = p.lns_sn
                AND l.acc_type = p.lns_type
                AND l.fk_unitcode = p.lns_open_unit
                AND p.prft_system = 4
                AND l.loan_status IN ('4')
                AND l.acc_status = '1'
                AND t.account_year =
                       EXTRACT (
                          YEAR FROM date_in))
    ,main0
     AS (SELECT   DISTINCT
                     min(final_interest)  AS amount
FROM     eom_loans
cross join config1
left join loan0 on (EOM_LOANS.ACC_SN = loan0.ACC_SN and eom_loans.ACC_TYPE=loan0.acc_type  and eom_loans.FK_UNITCODE =loan0.acc_unit)
         WHERE        eom_loans.id_product BETWEEN config1.product_from
                                               AND config1.product_to
                  AND eom_loans.fkgd_has_as_financ BETWEEN config1.finsc_from
                                                       AND config1.finsc_to
                  AND eom_loans.fkgd_has_as_class BETWEEN config1.ccode_from
                                                      AND config1.ccode_to
                  AND eom_loans.fkgd_has_as_loan_p BETWEEN config1.lpurp_from
                                                       AND config1.lpurp_to
                  AND eom_loans.fkgd_cbpurp BETWEEN config1.lpucb_from
                                                AND config1.lpucb_to
                  AND TRIM (eom_loans.loan_status) BETWEEN TRIM (
                                                              config1.loan_sts_from)
                                                       AND TRIM (
                                                              config1.loan_sts_to)
                  AND TRIM (eom_loans.loan_class) BETWEEN TRIM (
                                                             config1.classific_from)
                                                      AND TRIM (
                                                             config1.classific_to)
                  AND TRIM (eom_loans.final_sub_class) BETWEEN TRIM (config1.sub_class_from) AND TRIM ( config1.sub_class_to)
                  AND eom_loans.overdue_days BETWEEN nvl(config1.ov_days_from,0) AND nvl(config1.ov_days_to,99999)
                  AND fkgd_category BETWEEN nvl(config1.cust_categ_from,0)      AND nvl(config1.cust_categ_to,99999)
                  AND (   (config1.currency = 'ALL')
                       OR (    config1.currency = 'FC'
                           AND eom_loans.fkcur_is_moved_in IN (SELECT id_currency
                                                               FROM   currency
                                                               WHERE  national_flag =
                                                                         '0'))
                       OR     (config1.currency = 'DC')
                          AND eom_loans.fkcur_is_moved_in IN (SELECT id_currency
                                                              FROM   currency
                                                              WHERE  national_flag =
                                                                        '1'))
                  AND eom_date BETWEEN (CASE
                                           WHEN config1.date_from = 'CURRENT'
                                           THEN
                                              date_in /* Normal */
                                           WHEN config1.date_from = 'NOLIMIT'
                                           THEN to_date('01/01/1970','DD/MM/YYYY')
                                           WHEN config1.date_from =
                                                   'First Day of YEAR'
                                           THEN
                                              TRUNC (
                                                 date_in
                                                ,'YEAR')
                                           /* First Day of YEAR */
                                           WHEN config1.date_from =
                                                   'First Day of previous YEAR'
                                           THEN
                                              TO_DATE (
                                                    '01-01-'
                                                 || (  EXTRACT (
                                                          YEAR FROM date_in)
                                                     - 1)
                                                ,'DD/MM/YYYY')
                                           /* First Day of previous YEAR */
                                           WHEN config1.date_from =
                                                   'First Day two YEARS back'
                                           THEN
                                              TO_DATE (
                                                    '01-01-'
                                                 || (  EXTRACT (
                                                          YEAR FROM date_in)
                                                     - 2)
                                                ,'DD/MM/YYYY')
                                           /* First Day of two YEARS back */
                                           WHEN config1.date_from =
                                                   'Last Day of previous YEAR'
                                           THEN
                                              TO_DATE (
                                                    '31-12-'
                                                 || (  EXTRACT (
                                                          YEAR FROM date_in)
                                                     - 1)
                                                ,'DD/MM/YYYY')
                                        /* 31st Dec, Previous Year */
                                        END)
                                   AND (CASE
                                           WHEN config1.date_to = 'CURRENT'
                                           THEN
                                              date_in /* Normal */
                                           WHEN config1.date_to =
                                                   'First Day of YEAR'
                                           THEN
                                              TRUNC (
                                                 date_in
                                                ,'YEAR')
                                           /* First Day of YEAR */
                                           WHEN config1.date_to =
                                                   'First Day of previous YEAR'
                                           THEN
                                              TO_DATE (
                                                    '01-01-'
                                                 || (  EXTRACT (
                                                          YEAR FROM date_in)
                                                     - 1)
                                                ,'DD/MM/YYYY')
                                           /* First Day of previous YEAR */
                                           WHEN config1.date_to =
                                                   'Last Day of MONTH'
                                           THEN
                                              LAST_DAY (
                                                 date_in)
                                           /* Last Day of MONTH */
                                           WHEN config1.date_to =
                                                   'Last Day of previous YEAR'
                                           THEN
                                              TO_DATE (
                                                    '31-12-'
                                                 || (  EXTRACT (
                                                          YEAR FROM date_in)
                                                     - 1)
                                                ,'DD/MM/YYYY')
                                        /* 31st Dec, Previous Year */
                                        END)
         )
    ,main00
     AS (SELECT amount FROM main0
         UNION ALL
         SELECT 0
         FROM   sysibm.DUAL
         WHERE  NOT EXISTS (SELECT amount FROM main0))
SELECT round(sum(amount),0)as amount
FROM   main00
DO
  SET result0 = v1.amount;
END FOR;

return result0;

END;

