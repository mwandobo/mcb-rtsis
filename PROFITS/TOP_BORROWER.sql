CREATE PROCEDURE TOP_BORROWER (
    IN DATE_IN	DATE,
    IN REPORT_NAME_IN	VARCHAR(20),
    IN REPORT_SHEET_IN	VARCHAR(20),
    IN REPORT_CELL_IN	VARCHAR(20) )
  SPECIFIC SQL160728131632206
  LANGUAGE SQL
  NOT DETERMINISTIC
  EXTERNAL ACTION
  MODIFIES SQL DATA
  CALLED ON NULL INPUT
  INHERIT SPECIAL REGISTERS
  OLD SAVEPOINT LEVEL
 
   BEGIN
      DELETE FROM TMP_TOP_BORROWER;
      

INSERT INTO TMP_TOP_BORROWER
                (account_number,
                 account_cd,
                 agreement_number,
                 agreement_cd,
                 borrower_id,
                 borrower,
                 group_name,
                 product_desc,
                 finsc_desc,
                 acc_limit_amn_lc,
                 acc_limit_amn_fc,
                 lc_gross_total,
                 gross_total,
                 age_of_total,
                 loan_class,
                 product_dynamic_descr,
                 col_est_value_amn
 )
WITH 
config as 
(
        SELECT DISTINCT
        REPORT,
        EXCEL_SHEET,
        EXCEL_CELL,
            DATE_FROM,
            DATE_TO,
            AMT_TYPE,
            CURRENCY,
--            EXCEL_SHEET,
            trim(GL_ACCOUNT_FROM) as GL_ACCOUNT_FROM,
            trim(GL_ACCOUNT_TO) as GL_ACCOUNT_TO,
            FACTOR1
        FROM PROFITS_GL_CONFIGURATION
WHERE trim(REPORT) =  report_name_in
AND trim(EXCEL_SHEET) = report_sheet_in
AND trim(EXCEL_CELL) = report_cell_in
        and  REF_report is null
    union
        SELECT distinct
        g.REF_REPORT,
        g.REF_EXCEL_SHEET,
         g.REF_EXCEL_CELL,
            g1.DATE_FROM,
            g1.DATE_TO,
            g1.AMT_TYPE,
            g1.CURRENCY,
--            g1.EXCEL_SHEET,
            trim(g1.GL_ACCOUNT_FROM) as GL_ACCOUNT_FROM,
            trim(g1.GL_ACCOUNT_TO) as GL_ACCOUNT_TO,
            g1.FACTOR1*g.FACTOR1 as FACTOR1
        FROM PROFITS_GL_CONFIGURATION g, PROFITS_GL_CONFIGURATION g1
        WHERE g.REPORT =  report_name_in
        AND g.EXCEL_SHEET = report_sheet_in
        AND g.EXCEL_CELL =  report_cell_in
        and g1.REPORT =  g.REF_REPORT
        AND g1.EXCEL_SHEET = g.REF_EXCEL_SHEET
        AND g1.EXCEL_CELL = g.REF_EXCEL_CELL
),
core as
(
--select 1 core_amnt from sysibm.dual
    SELECT sum(MCB_REPORT_PKG.GET_GL_BALANCES_CUR_UNIT (config.REPORT,config.EXCEL_SHEET,config.EXCEL_CELL,date_in,'ALL','ALL'))as core_amnt FROM config
),
hierarchy0
     AS (SELECT DISTINCT eom_date
                        ,acct_key
                        ,lower_acct_key
                        ,upper_acct_key
         FROM   w_eom_set_acct_hierarchy)
    ,coll0
     AS (SELECT DISTINCT eom_loans.account_number
                        ,eom_loans.agreement_number
                        ,eom_loans.cust_id
                        ,w_eom_collateral.product_code
                        ,w_eom_collateral.product_dynamic_descr
                        ,w_eom_collateral.col_est_value_amn
         FROM   w_fact_acct_collat_limit f
                LEFT JOIN hierarchy0 ON hierarchy0.acct_key = f.acct_key
                INNER JOIN eom_loans ON (eom_loans.cust_id = f.cust_id)
                INNER JOIN w_eom_collateral
                   ON (    f.collat_combo_key = w_eom_collateral.combo_key
                       AND w_eom_collateral.eom_date BETWEEN f.eff_from_date
                                                         AND f.eff_to_date)
                INNER JOIN product
                   ON (product.id_product = w_eom_collateral.product_code)
         WHERE      hierarchy0.eom_date =  date_in
                AND collateral_status = '1')
    ,dynamic0
     AS (SELECT   DISTINCT
       /* account_number LISTAGG (TRIM (product_dynamic_descr), ', ') WITHIN GROUP (ORDER BY product_dynamic_descr) OVER (PARTITION BY account_number)  AS product_dynamic_descr
                FROM     coll0
         GROUP BY account_number, product_dynamic_descr) */
                account_number, LISTAGG (distinct  (product_dynamic_descr), ', ') WITHIN GROUP (ORDER BY product_dynamic_descr)  as product_dynamic_descr
                from ( select account_number, product_dynamic_descr, row_number() over (partition by account_number order by account_number) as rnum 
                        from  coll0 
                      )                
                where rnum = 1 
          group by account_number)     
    ,final0
     AS (SELECT DISTINCT
                eom_loans.account_number
               ,eom_loans.account_cd
               ,eom_loans.agreement_number
               ,eom_loans.agreement_cd
               ,--    v.account_number AS agreement_number,
                v.cust_id AS borrower_id
               ,TRIM (customer.surname) || ' ' || TRIM (customer.first_name)
                   AS borrower
               ,--    CUSTOMER_NAME,
                NVL (cust_group.description, ' ') AS group_name
               ,UPPER (eom_loans.product_desc) AS product_desc
               ,UPPER (generic_detail.description) AS finsc_desc
               , (CASE
                     WHEN currency = (SELECT short_descr
                                      FROM   currency
                                      WHERE  national_flag = '1')
                     THEN
                        acc_limit_amn * fixing_rate
                     ELSE
                        0
                  END)
                   AS acc_limit_amn_lc
               , (CASE
                     WHEN currency <> (SELECT short_descr
                                       FROM   currency
                                       WHERE  national_flag = '1')
                     THEN
                        acc_limit_amn
                     ELSE
                        0
                  END)
                   AS acc_limit_amn_fc
               ,eom_loans.lc_gross_total
               ,eom_loans.gross_total
               , (eom_loans.lc_gross_total + eom_loans.gross_total) / 1000000
                   AS age_of_total
               , (CASE
                     WHEN loan_class = '1' THEN 'NON-PERFORMING'
                     WHEN loan_class = '0' THEN 'PERFORMING'
                     ELSE NULL
                  END)
                   AS loan_class
               ,NVL (dynamic0.product_dynamic_descr, ' ')
                   AS product_dynamic_descr
               ,NVL (coll0.col_est_value_amn, 0) * fixing_rate
                   AS col_est_value_amn
         FROM   loans_benefs_vw v
                INNER JOIN eom_loans
                   ON (    (eom_loans.cust_id = v.cust_id /*OR eom_Loans.cust_id = v.main_benef_cust_i */
                                                         )
                       AND eom_loans.agreement_number = v.account_number)
                INNER JOIN customer ON (customer.cust_id = v.cust_id)
                LEFT JOIN cust_group_member
                   ON (cust_group_member.fk_customercust_id = v.cust_id)
                LEFT JOIN cust_group
                   ON (cust_group.GROUP_ID =
                          cust_group_member.fk_cust_groupgroup)
                LEFT JOIN generic_detail
                   ON (    generic_detail.fk_generic_headpar =
                              eom_loans.fkgh_has_as_financ
                       AND generic_detail.serial_num =
                              eom_loans.fkgd_has_as_financ AND fkgh_has_as_financ = 'FINSC')
                LEFT JOIN coll0
                   ON (eom_loans.account_number = coll0.account_number)
                LEFT JOIN dynamic0
                   ON (eom_loans.account_number = dynamic0.account_number)
         WHERE      eom_loans.eom_date = date_in
                AND v.benef_status = '1')
SELECT   DISTINCT account_number
                 ,account_cd
                 ,agreement_number
                 ,agreement_cd
                 ,borrower_id
                 ,borrower
                 ,group_name
                 ,product_desc
                 ,finsc_desc
                 ,decimal(acc_limit_amn_lc,15,2) as acc_limit_amn_lc
                 ,acc_limit_amn_fc
                 ,lc_gross_total
                 ,gross_total
                 ,age_of_total
                 ,loan_class
                 ,product_dynamic_descr
                 ,decimal(SUM (col_est_value_amn),15,2) as col_est_value_amn
FROM     final0
WHERE
gross_total >= (select core_amnt from core)
GROUP BY account_number
        ,account_cd
        ,agreement_number
        ,agreement_cd
        ,borrower_id
        ,borrower
        ,group_name
        ,product_desc
        ,finsc_desc
        ,acc_limit_amn_lc
        ,acc_limit_amn_fc
        ,lc_gross_total
        ,gross_total
        ,age_of_total
        ,loan_class
        ,product_dynamic_descr;
        
        
        END;

