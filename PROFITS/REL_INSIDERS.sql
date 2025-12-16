CREATE PROCEDURE REL_INSIDERS ( IN DATE_IN DATE )
  SPECIFIC SQL160728131624305
  LANGUAGE SQL
  NOT DETERMINISTIC
  EXTERNAL ACTION
  MODIFIES SQL DATA
  CALLED ON NULL INPUT
  INHERIT SPECIAL REGISTERS
  OLD SAVEPOINT LEVEL
   BEGIN
      DELETE FROM TMP_REL_INSIDER;

      

INSERT INTO TMP_REL_INSIDER
WITH hierarchy0
     AS (SELECT DISTINCT eom_date
                        ,acct_key
                        ,lower_acct_key
                        ,upper_acct_key
         FROM   w_eom_set_acct_hierarchy)
    ,cust_categ0
     AS (SELECT fk_customercust_id
               ,fk_categorycategor
               ,fk_generic_detaser
               ,fk_generic_detafk
               ,description
         FROM   customer_category
                INNER JOIN generic_detail
                   ON (    generic_detail.fk_generic_headpar = 'INSID'
                       AND generic_detail.serial_num = fk_generic_detaser)
         WHERE  customer_category.fk_categorycategor = 'INSIDER')
    ,main0
     AS (SELECT DISTINCT r1.fkcust_has_as_firs AS borrower_id
                        ,r1.fkcust_has_as_seco AS related_id
                        ,cust_categ0.description AS related
                        ,rel_description AS rel_description
         FROM   cust_categ0
                INNER JOIN relationship r1
                   ON (cust_categ0.fk_customercust_id = r1.fkcust_has_as_firs)
                INNER JOIN relationship_type rt1
                   ON (rt1.type_id = r1.fk_relationshiptyp)
         UNION
         SELECT DISTINCT r2.fkcust_has_as_seco AS borrower_id
                        ,r2.fkcust_has_as_firs AS related_id
                        ,cust_categ0.description AS related
                        ,other_rel_descript AS rel_description
         FROM   cust_categ0
                INNER JOIN relationship r2
                   ON (cust_categ0.fk_customercust_id = r2.fkcust_has_as_seco)
                INNER JOIN relationship_type rt2
                   ON (rt2.type_id = r2.fk_relationshiptyp))
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
         WHERE  hierarchy0.eom_date = date_in
         and COLLATERAL_STATUS = '1')
    ,dynamic0
     AS (SELECT   DISTINCT
                  account_number,
--                 ,LISTAGG (trim(product_dynamic_descr), ', ')
--                     WITHIN GROUP (ORDER BY product_dynamic_descr)
 --                    OVER (PARTITION BY account_number)
                   ' '  AS product_dynamic_descr
         FROM     coll0
         GROUP BY account_number, product_dynamic_descr)
    ,final0
     AS (SELECT DISTINCT
                eom_loans.account_number
               ,eom_loans.account_cd
               ,eom_loans.agreement_number
               ,eom_loans.agreement_cd
               ,borrower_id
               ,TRIM (b.surname) || ' ' || TRIM (b.first_name)
                   AS borrower_name
               ,related_id
               ,TRIM (r.surname) || ' ' || TRIM (r.first_name)
                   AS related_name
               ,related
               ,rel_description
               ,--     EOM_LOANS.ID_PRODUCT,
                UPPER (eom_loans.product_desc) AS product_desc
               ,NVL (eom_loans.acc_limit_amn, 0) AS acc_limit_amn
               ,eom_loans.acc_exp_dt
               ,NVL (eom_loans.final_interest, 0) AS final_interest
               ,--     EOM_LOANS.GROSS_TOTAL*EOM_LOANS.FIXING_RATE as GOSS_TOLAL,
                eom_loans.lc_gross_total
               , (CASE
                     WHEN loan_class = '1' THEN 'NON-PERFORMING'
                     WHEN loan_class = '0' THEN 'PERFORMING'
                     ELSE NULL
                  END)
                   AS loan_class
               ,NVL (dynamic0.product_dynamic_descr, ' ')
                   AS product_dynamic_descr
               ,NVL (coll0.col_est_value_amn, 0) AS col_est_value_amn
         FROM   main0
                INNER JOIN loans_benefs_vw v
                   ON (v.cust_id = main0.borrower_id)
                INNER JOIN eom_loans
                   ON (    (eom_loans.cust_id = main0.borrower_id)
                       AND eom_loans.agreement_number = v.account_number)
                INNER JOIN customer b ON (b.cust_id = main0.borrower_id)
                INNER JOIN customer r ON (r.cust_id = main0.related_id)
                LEFT JOIN coll0
                   ON (eom_loans.account_number = coll0.account_number)
                LEFT JOIN dynamic0
                   ON (eom_loans.account_number = dynamic0.account_number)
         WHERE      eom_loans.eom_date = date_in
                AND v.benef_status = '1')
SELECT   DISTINCT --    ACCOUNT_NUMBER,
                  --    ACCOUNT_CD,
                  --    AGREEMENT_NUMBER,
                  --    AGREEMENT_CD,
                  --    borrower_id,
                  borrower_name
                 ,--    related_id,
                  related_name
                 ,related
                 ,rel_description
                 ,product_desc
                 ,acc_limit_amn
                 ,acc_exp_dt
                 ,final_interest
                 ,lc_gross_total
                 ,loan_class
                 ,product_dynamic_descr
                 ,SUM (col_est_value_amn)
FROM     final0
GROUP BY --    ACCOUNT_NUMBER,
         --    ACCOUNT_CD,
         --    AGREEMENT_NUMBER,
         --    AGREEMENT_CD,
         --    borrower_id,
         borrower_name
        ,--    related_id,
         related_name
        ,related
        ,product_desc
        ,rel_description
        ,acc_limit_amn
        ,acc_exp_dt
        ,final_interest
        ,loan_class
        ,lc_gross_total
        ,product_dynamic_descr
ORDER BY borrower_name;


      
   END;

