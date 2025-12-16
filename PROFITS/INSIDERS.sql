CREATE PROCEDURE INSIDERS ( IN DATE_IN DATE )
  SPECIFIC SQL160728131624004
  LANGUAGE SQL
  NOT DETERMINISTIC
  EXTERNAL ACTION
  MODIFIES SQL DATA
  CALLED ON NULL INPUT
  INHERIT SPECIAL REGISTERS
  OLD SAVEPOINT LEVEL
   BEGIN
      DELETE FROM TMP_INSIDER2;

INSERT INTO TMP_INSIDER2
      WITH hierarchy0
           AS (SELECT DISTINCT eom_date
                              ,acct_key
                              ,lower_acct_key
                              ,upper_acct_key
               FROM   w_eom_set_acct_hierarchy)
          ,main0
           AS (SELECT DISTINCT v.account_number AS agreement_number
                              ,t.fkcust_has_as_seco AS insider
                              ,v.cust_id AS borrower
                              ,customer_category.fk_generic_detafk
                              ,customer_category.fk_generic_detaser
               FROM   relationship t
                      INNER JOIN customer_category
                         ON (    customer_category.fk_customercust_id =
                                    t.fkcust_has_as_seco
                             AND customer_category.fk_categorycategor =
                                    'INSIDER')
                      INNER JOIN loans_benefs_vw v
                         ON     (   v.cust_id = fkcust_has_as_seco
                                 OR v.cust_id = fkcust_has_as_firs)
                            AND fk_relationshiptyp = 'INSIDER'
               UNION
               SELECT DISTINCT account_number AS agreement_number
                              ,fkcust_has_as_seco AS insider
                              ,v.cust_id AS borrower
                              ,'INSID' fk_generic_detafk
                              ,1 fk_generic_detaser
               FROM   relationship
                      INNER JOIN loans_benefs_vw v
                         ON (   v.cust_id = fkcust_has_as_seco
                             OR v.cust_id = fkcust_has_as_firs)
               WHERE      fkcust_has_as_seco NOT IN (SELECT fk_customercust_id
                                                     FROM   customer_category
                                                     WHERE  fk_categorycategor =
                                                               'INSIDER')
                      AND fk_relationshiptyp = 'INSIDER'
               UNION
               SELECT DISTINCT vv.account_number AS agreement_number
                              ,fkcust_has_as_seco AS insider
                              ,vv.cust_id AS borrower
                              ,'INSID' fk_generic_detafk
                              ,1 fk_generic_detaser
               FROM   relationship
                      INNER JOIN loans_benefs_vw v
                         ON (   v.cust_id = fkcust_has_as_seco
                             OR v.cust_id = fkcust_has_as_firs)
                      INNER JOIN loans_benefs_vw vv
                         ON (   vv.cust_id = v.cust_id
                             OR vv.main_benef_cust_id = v.main_benef_cust_id)
               WHERE      fkcust_has_as_seco NOT IN (SELECT fk_customercust_id
                                                     FROM   customer_category
                                                     WHERE  fk_categorycategor =
                                                               'INSIDER')
                      AND fk_relationshiptyp = 'INSIDER'
               UNION
               SELECT DISTINCT account_number AS agreement_number
                              ,fk_customercust_id AS insider
                              ,v.cust_id AS borrower
                              ,customer_category.fk_generic_detafk
                              ,customer_category.fk_generic_detaser
               FROM   customer_category
                      INNER JOIN loans_benefs_vw v
                         ON (v.cust_id = customer_category.fk_customercust_id)
               WHERE  fk_categorycategor = 'INSIDER')
          ,extra0
           AS (SELECT   DISTINCT
                        v.account_number AS agreement_number, v.cust_id
               FROM     loans_benefs_vw v
                        INNER JOIN main0
                           ON (    (   main0.borrower = v.cust_id
                                    OR main0.borrower = v.main_benef_cust_id)
                               AND main0.agreement_number = v.account_number)
               --                     INNER JOIN main0  ON (main0.agreement_number = v.account_number)
               WHERE    v.benef_status = '1'
               GROUP BY v.account_number, v.cust_id)
          ,cust0
           AS (SELECT DISTINCT
                      extra0.cust_id
                     ,TRIM (c2.first_name) || ' ' || TRIM (c2.surname)
                         AS name0
                     ,extra0.agreement_number
               FROM   extra0
                      INNER JOIN eom_loans
                         ON (eom_loans.agreement_number =
                                extra0.agreement_number)
                      INNER JOIN customer c2 ON (c2.cust_id = extra0.cust_id)
               WHERE  eom_loans.eom_date =
                         date_in)
          ,name0
           AS (SELECT   DISTINCT
--                        RTRIM (
--                           XMLAGG (XMLELEMENT (e, name0 || ', ')).EXTRACT (
--                              '//text()'))
                         ' '   AS borrower_name -- null AS borrower_name
                       ,agreement_number
               FROM     cust0
               GROUP BY agreement_number)
           , coll0
           AS (SELECT DISTINCT eom_loans.account_number
                              ,eom_loans.agreement_number
                              ,eom_loans.cust_id
                              ,w_eom_collateral.product_code
                              ,w_eom_collateral.product_dynamic_descr
                              ,w_eom_collateral.col_est_value_amn
               FROM   w_fact_acct_collat_limit f
                      LEFT JOIN hierarchy0
                         ON hierarchy0.acct_key = f.acct_key
                      INNER JOIN eom_loans ON (eom_loans.cust_id = f.cust_id)
                      INNER JOIN w_eom_collateral
                         ON (    f.collat_combo_key =
                                    w_eom_collateral.combo_key
                             AND w_eom_collateral.eom_date BETWEEN f.eff_from_date
                                                               AND f.eff_to_date)
                      INNER JOIN product
                         ON (product.id_product =
                                w_eom_collateral.product_code)
               WHERE  hierarchy0.eom_date = date_in
               and COLLATERAL_STATUS = '1')
          ,dynamic0
           AS (SELECT   DISTINCT
                        account_number,
--                       ,LISTAGG (trim(product_dynamic_descr), ', ')
--                           WITHIN GROUP (ORDER BY product_dynamic_descr)
--                           OVER (PARTITION BY account_number)
                        ' ' AS product_dynamic_descr --  NULL AS product_dynamic_descr
               FROM     coll0
               GROUP BY account_number, product_dynamic_descr
               )
      SELECT   DISTINCT
               name0.borrower_name
              ,TRIM (c1.first_name) || ' ' || TRIM (c1.surname) AS insider_name
              ,generic_detail.description AS insider_desc
              ,eom_loans.acc_limit_amn
              ,eom_loans.acc_exp_dt
              ,eom_loans.final_interest
              ,eom_loans.lc_gross_total
              ,eom_loans.agreement_number
              ,eom_loans.account_number
              , (CASE
                    WHEN loan_class = '1' THEN 'Non-performing'
                    WHEN loan_class = '0' THEN 'Performing'
                    ELSE NULL
                 END)
                  AS loan_class
              ,--            coll0.PRODUCT_DYNAMIC_DESCR,
               dynamic0.product_dynamic_descr
              ,SUM (coll0.col_est_value_amn) AS col_est_value_amn
      FROM     eom_loans
               INNER JOIN main0
                  ON (main0.agreement_number = eom_loans.agreement_number)
               INNER JOIN name0
                  ON (name0.agreement_number = eom_loans.agreement_number)
               LEFT JOIN generic_detail
                  ON (    main0.fk_generic_detafk =
                             generic_detail.fk_generic_headpar
                      AND main0.fk_generic_detaser = generic_detail.serial_num)
               INNER JOIN customer c1 ON (c1.cust_id = main0.insider)
               left JOIN coll0
                  ON (eom_loans.account_number = coll0.account_number)
               left JOIN dynamic0
                  ON (eom_loans.account_number = dynamic0.account_number)
      WHERE    eom_loans.eom_date = date_in
      GROUP BY name0.borrower_name
              ,c1.first_name
              ,c1.surname
              ,generic_detail.description
              ,eom_loans.loan_class
              ,eom_loans.agreement_number
              ,eom_loans.account_number
              ,main0.fk_generic_detafk
              ,main0.fk_generic_detaser
              ,eom_loans.acc_limit_amn
              ,eom_loans.acc_exp_dt
              ,eom_loans.final_interest
              ,eom_loans.lc_gross_total
              ,--            PRODUCT_DYNAMIC_DESCR
               dynamic0.product_dynamic_descr
      --            coll0.PRODUCT_DYNAMIC_DESCR
      ORDER BY name0.borrower_name ASC;

      
   END;

