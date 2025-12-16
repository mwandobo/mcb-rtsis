CREATE PROCEDURE RUN_LOAN_AGREEMENT_NEW ( IN P_PROGRAM_ID VARCHAR(10) )
BEGIN
     DECLARE l_countc02 INTEGER;
     DECLARE l_serialnum DECFLOAT;
     DECLARE L_TEMP_AMOUNT DECFLOAT;
     DECLARE L_FK_UNITCODE        INT;
     DECLARE L_acc_type           SMALLINT;
     DECLARE L_acc_sn             INT;
     DECLARE L_CUST_ID            INT;
     DECLARE L_FK_LOANFK_PRODUCTI INT;
     DECLARE L_FKGD_HAS_AS_FINANC INT;
     DECLARE L_FKGD_HAS_AS_CLASS  INT;
     DECLARE L_FKGD_CATEGORY      INT;
     DECLARE L_FKCUR_IS_MOVED_IN  INT;
     DECLARE L_LOAN_STATUS        CHAR ( 1 );
     DECLARE L_LOAN_GCURR         SMALLINT;
     DECLARE L_LOAN_DURA          SMALLINT;
     DECLARE L_LOAN_STAT          SMALLINT;
     DECLARE L_LOAN_CLASS         SMALLINT;
     DECLARE L_LOAN_EXP           SMALLINT;
     DECLARE L_LOAN_COMM          SMALLINT;
     DECLARE L_LOAN_TAX           SMALLINT;
     DECLARE L_FKGD_HAS_A_PRIMARY INT;
     DECLARE L_GROUP              CHAR ( 4 );
     DECLARE L_AMOUNT             DECIMAL ( 15 , 2 );
     DECLARE L_ACCOUNT_NUMBER     CHAR ( 40 );
     DECLARE L_MONOTORING_UNIT    INT;
     DECLARE L_DURATIONMIN        INT;
     DECLARE L_GLG_ACCOUNT        CHAR ( 21 );
     DECLARE L_CHARGE_TYPE        INT;
     DECLARE L_CHARGE_CODE        INT;
   FOR c02
      AS (SELECT COUNT (*) countc02
          FROM   par_relation_detai a, par_relation_detai b
          WHERE      a.fk_par_relationcod = 'CATAGR'
                 AND a.fkgh_has_a_seconda = 'SUBSY'
                 AND a.fkgh_has_a_primary = 'ACROR'
                 AND a.fkgd_has_a_seconda = '4' --LOANS SUBSYSTEM;
                 AND b.fk_par_relationcod = 'CATORK'
                 AND b.fkgh_has_a_primary = 'ACROK'
                 AND b.fkgh_has_a_seconda = 'ACROR'
                 AND b.fkgd_has_a_primary = '1' --ACCOUNT TYPE ORIGIN
                 AND b.fkgd_has_a_seconda = a.fkgd_has_a_primary)
   DO
      SET l_countc02 = c02.countc02;
   END FOR;
   SET l_serialnum = 0;
   FOR c1
      AS (SELECT   a.fk_unitcode
                  ,a.acc_type
                  ,a.acc_sn
                  ,a.cust_id
                  ,a.fk_loanfk_producti
                  ,DECODE (b.loan_finsc, '0', '0', a.fkgd_has_as_financ)
                      fkgd_has_as_financ
                  ,DECODE (b.loan_ccode, '0', '0', a.fkgd_has_as_class)
                      fkgd_has_as_class
                  ,DECODE (b.loan_cloan, '0', '0', a.fkgd_category)
                      fkgd_category
                  ,a.fkcur_is_moved_in
                  ,a.loan_status
                  ,b.loan_finsc
                  ,b.loan_ccode
                  ,b.loan_cloan
                  ,b.loan_gcurr
                  ,b.loan_dura
                  ,b.loan_stat
                  ,b.loan_class
                  ,b.loan_exp
                  ,b.loan_comm
                  ,b.loan_tax
                  ,p.account_number
                  ,p.monotoring_unit
          FROM     r_loan_account a
                  ,loan b
                  ,batch_parameters c
                  ,profits_account p
          WHERE        (a.acc_status = '1' --active
                                          OR a.acc_status = '2') --blocked
                   AND a.loan_status <> '4' --write off
                   AND b.fk_productid_produ = a.fk_loanfk_producti
                   AND c.program_id = p_program_id
                   AND a.cust_id BETWEEN c.customer_from AND c.customer_to
                   AND p.lns_open_unit = a.fk_unitcode
                   AND p.lns_type = a.acc_type
                   AND p.lns_sn = a.acc_sn
          ORDER BY fk_unitcode, acc_type, acc_sn)
   DO
      SET l_fk_unitcode = c1.fk_unitcode;
      SET l_acc_type = c1.acc_type;
      SET l_acc_sn = c1.acc_sn;
      SET l_cust_id = c1.cust_id;
      SET l_fk_loanfk_producti = c1.fk_loanfk_producti;
      SET l_fkgd_has_as_financ = c1.fkgd_has_as_financ;
      SET l_fkgd_has_as_class = c1.fkgd_has_as_class;
      SET l_fkgd_category = c1.fkgd_category;
      SET l_fkcur_is_moved_in = c1.fkcur_is_moved_in;
      SET l_loan_status = c1.loan_status;
      SET l_loan_gcurr = c1.loan_gcurr;
      SET l_loan_dura = c1.loan_dura;
      SET l_loan_stat = c1.loan_stat;
      SET l_loan_class = c1.loan_class;
      SET l_loan_exp = c1.loan_exp;
      SET l_loan_comm = c1.loan_comm;
      SET l_loan_tax = c1.loan_tax;
      SET l_account_number = c1.account_number;
      SET l_monotoring_unit = c1.monotoring_unit;
      IF l_loan_gcurr = '0'
      THEN
         SET l_group = '    ';
      ELSE
         FOR c3
            AS (SELECT fk_glg_h_curr_ggro
                FROM   glg_d_curr_group, glg_h_curr_group
                WHERE      glg_h_curr_group.GROUP_ID =
                              glg_d_curr_group.fk_glg_h_curr_ggro
                       AND glg_d_curr_group.fk_currencyid_curr =
                              l_fkcur_is_moved_in
                       AND glg_h_curr_group.class_flag = '1')
         DO
            SET l_group = c3.fk_glg_h_curr_ggro;
         END FOR;
      END IF;
      FOR c4
         AS (SELECT MIN (a.duration_days_to) durationmin
             FROM   class_gl_td_h_det a
                   ,generic_detail finsc
                   ,generic_detail ccode
                   ,generic_detail cloan
                   ,glg_h_curr_group d
                   ,loan_add_info b
                   ,loan_account_info c
             WHERE      a.fk_productid_produ = l_fk_loanfk_producti
                    AND finsc.fk_generic_headpar = 'FINSC'
                    AND a.fk_gen_det_finscv = finsc.serial_num
                    AND finsc.serial_num = l_fkgd_has_as_financ
                    AND ccode.fk_generic_headpar = 'CCODE'
                    AND a.fk_generic_detaser = ccode.serial_num
                    AND ccode.serial_num = l_fkgd_has_as_class
                    AND cloan.fk_generic_headpar = 'CLOAN'
                    AND a.fk_cust_categ_gd = cloan.serial_num
                    AND cloan.serial_num = l_fkgd_category
                    AND a.fk_glg_h_curr_ggro = d.GROUP_ID
                    AND (   l_loan_dura = '0'
                         OR (    a.duration_days_to >= b.amount_data
                             AND l_loan_dura = '1'))
                    AND b.acc_unit = l_fk_unitcode
                    AND b.acc_type = l_acc_type
                    AND b.acc_sn = l_acc_sn
                    AND b.row_id = 25
                    AND c.fk_loan_accountfk = l_fk_unitcode
                    AND c.fk0loan_accountacc = l_acc_type
                    AND c.fk_loan_accountacc = l_acc_sn
                    AND d.GROUP_ID = l_group
                    AND (   l_loan_stat = '0'
                         OR (    l_loan_stat = '1'
                             AND a.accnt_status = l_loan_status))
                    AND (   l_loan_class = '0'
                         OR (    l_loan_class = '1'
                             AND a.accnt_class = c.loan_class)))
      DO
         SET l_durationmin = c4.durationmin;
      END FOR;
      IF l_countc02 = 0
      THEN
         SET l_serialnum = l_serialnum + 1;
         INSERT INTO rep_74220_det_exp (
                        serialnum
                       ,acc_unit
                       ,acc_move_curr
                       ,gl_account
                       ,prft_system
                       ,prof_account
                       ,origin_id
                       ,gl_balance
                       ,errormessage
                       ,origin_type
                       ,charge_code
                       ,tmstamp)
            VALUES      (
                           l_serialnum
                          ,' '
                          ,' '
                          ,' '
                          ,'4'
                          ,''
                          ,''
                          ,0
                          ,'THERE ARE NOT ORIGINS FOR ORIGIN_TYPE: ACCOUNTS IN PARAMETER CATAGR -PAR_RELATION_DETAI- FOR SUBSYSTEM 4!'
                          ,'1'
                          ,'0'
                          ,current TIMESTAMP);
      ELSE
         FOR c2
            AS (SELECT a.fkgd_has_a_primary
                FROM   par_relation_detai a, par_relation_detai b
                WHERE      a.fk_par_relationcod = 'CATAGR'
                       AND a.fkgh_has_a_seconda = 'SUBSY'
                       AND a.fkgh_has_a_primary = 'ACROR'
                       AND a.fkgd_has_a_seconda = '4' --LOANS SUBSYSTEM;
                       AND b.fk_par_relationcod = 'CATORK'
                       AND b.fkgh_has_a_primary = 'ACROK'
                       AND b.fkgh_has_a_seconda = 'ACROR'
                       AND b.fkgd_has_a_primary = '1' --ACCOUNT TYPE ORIGIN
                       AND b.fkgd_has_a_seconda = a.fkgd_has_a_primary)
         DO
            SET l_fkgd_has_a_primary = c2.fkgd_has_a_primary;
            SET l_charge_type = '1';
            FOR c41
               AS (SELECT a.fk_glg_account
                   FROM   class_gl_td_h_det a
                         ,generic_detail finsc
                         ,generic_detail ccode
                         ,generic_detail cloan
                         ,glg_h_curr_group d
                         ,loan_add_info b
                         ,loan_account_info c
                   WHERE      a.fk_productid_produ = l_fk_loanfk_producti
                          AND finsc.fk_generic_headpar = 'FINSC'
                          AND a.fk_gen_det_finscv = finsc.serial_num
                          AND finsc.serial_num = l_fkgd_has_as_financ
                          AND ccode.fk_generic_headpar = 'CCODE'
                          AND a.fk_generic_detaser = ccode.serial_num
                          AND ccode.serial_num = l_fkgd_has_as_class
                          AND cloan.fk_generic_headpar = 'CLOAN'
                          AND a.fk_cust_categ_gd = cloan.serial_num
                          AND cloan.serial_num = l_fkgd_category
                          AND a.fk_glg_h_curr_ggro = d.GROUP_ID
                          AND (   l_loan_dura = '0'
                               OR (    a.duration_days_to >= b.amount_data
                                   AND l_loan_dura = '1'))
                          AND b.acc_unit = l_fk_unitcode
                          AND b.acc_type = l_acc_type
                          AND b.acc_sn = l_acc_sn
                          AND b.row_id = 25
                          AND c.fk_loan_accountfk = l_fk_unitcode
                          AND c.fk0loan_accountacc = l_acc_type
                          AND c.fk_loan_accountacc = l_acc_sn
                          AND a.origin_id = l_fkgd_has_a_primary
                          AND a.origin_type = l_charge_type
                          AND d.GROUP_ID = l_group
                          AND (   l_loan_stat = '0'
                               OR (    l_loan_stat = '1'
                                   AND a.accnt_status = l_loan_status))
                          AND (   l_loan_class = '0'
                               OR (    l_loan_class = '1'
                                   AND a.accnt_class = c.loan_class))
                          AND a.duration_days_to = l_durationmin)
            DO
               SET l_glg_account = c41.fk_glg_account;
            END FOR;
            SET l_temp_amount = 0;
            FOR c5
               AS (SELECT   SUM(get_amount_for_agr (
                               sql_statement
                              ,table_name
                              ,l_fk_unitcode
                              ,l_acc_type
                              ,l_acc_sn
                              ,''
                              ,''
                              ,0))
                               amount
                   FROM     class_gl_td_origin
                   WHERE        prft_system = '4'
                            AND origin_id = l_fkgd_has_a_primary)
            DO
               SET l_amount = c5.amount;
               SET l_temp_amount = l_temp_amount + l_amount;
            END FOR;
            IF l_temp_amount <> 0
            THEN
               SET l_serialnum = l_serialnum + 1;
               IF l_glg_account IS NOT NULL
               THEN
                  INSERT INTO rep_74220_det (
                                 serialnum
                                ,prft_system
                                ,gl_account
                                ,acc_move_curr
                                ,acc_unit
                                ,prof_account
                                ,gl_balance
                                ,origin_id
                                ,origin_type
                                ,charge_code)
                  VALUES      (
                                 l_serialnum
                                ,'4'
                                ,l_glg_account
                                ,l_fkcur_is_moved_in
                                ,l_monotoring_unit
                                ,l_account_number
                                ,l_temp_amount
                                ,l_fkgd_has_a_primary
                                ,'1'
                                ,'0');
               ELSE
                  INSERT INTO rep_74220_det_exp (
                                 serialnum
                                ,acc_unit
                                ,acc_move_curr
                                ,gl_account
                                ,prft_system
                                ,prof_account
                                ,origin_id
                                ,gl_balance
                                ,errormessage
                                ,origin_type
                                ,charge_code
                                ,tmstamp)
                  VALUES      (
                                 l_serialnum
                                ,l_monotoring_unit
                                ,l_fkcur_is_moved_in
                                ,' '
                                ,'4'
                                ,l_account_number
                                ,l_fkgd_has_a_primary
                                ,l_temp_amount
                                ,'GL ACCOUNT IS NOT DEFINED!'
                                ,'1'
                                ,'0'
                                ,current TIMESTAMP);
               END IF;
            END IF;
         END FOR;
      END IF;
      FOR c200
         AS (SELECT a.fkgd_has_a_primary, b.fkgd_has_a_primary charge_type
             FROM   par_relation_detai a, par_relation_detai b
             WHERE      a.fk_par_relationcod = 'CATAGR'
                    AND a.fkgh_has_a_seconda = 'SUBSY'
                    AND a.fkgh_has_a_primary = 'ACROR'
                    AND a.fkgd_has_a_seconda = '4' --LOANS SUBSYSTEM;
                    AND b.fk_par_relationcod = 'CATORK'
                    AND b.fkgh_has_a_primary = 'ACROK'
                    AND b.fkgh_has_a_seconda = 'ACROR'
                    AND b.fkgd_has_a_primary <> '1' --NOT ACCOUNT TYPE ORIGIN:TAX/COMMISSION/EXPENSE
                    AND b.fkgd_has_a_seconda = a.fkgd_has_a_primary)
      DO
         SET l_fkgd_has_a_primary = c200.fkgd_has_a_primary;
         SET l_charge_type = c200.charge_type;
         FOR c42
            AS (SELECT   DISTINCT charge_code
                FROM     cust_acc_charges
                WHERE        account_number = l_account_number
                         AND prft_system = '4'
                         AND charge_type = l_charge_type
                GROUP BY charge_code)
         DO
            SET l_charge_code = c42.charge_code;
            SET l_glg_account = NULL;
            FOR c4100
               AS (SELECT a.fk_glg_account
                   FROM   class_gl_td_h_det a
                         ,generic_detail finsc
                         ,generic_detail ccode
                         ,generic_detail cloan
                         ,glg_d_curr_group d
                         ,loan_add_info b
                         ,loan_account_info c
                   WHERE      a.fk_productid_produ = l_fk_loanfk_producti
                          AND finsc.fk_generic_headpar = 'FINSC'
                          AND a.fk_gen_det_finscv = finsc.serial_num
                          AND finsc.serial_num = l_fkgd_has_as_financ
                          AND ccode.fk_generic_headpar = 'CCODE'
                          AND a.fk_generic_detaser = ccode.serial_num
                          AND ccode.serial_num = l_fkgd_has_as_class
                          AND cloan.fk_generic_headpar = 'CLOAN'
                          AND a.fk_cust_categ_gd = cloan.serial_num
                          AND cloan.serial_num = l_fkgd_category
                          AND a.fk_glg_h_curr_ggro = l_group
                          AND (   l_loan_dura = '0'
                               OR (    a.duration_days_to >= b.amount_data
                                   AND l_loan_dura = '1'))
                          AND b.acc_unit = l_fk_unitcode
                          AND b.acc_type = l_acc_type
                          AND b.acc_sn = l_acc_sn
                          AND b.row_id = 25
                          AND c.fk_loan_accountfk = l_fk_unitcode
                          AND c.fk0loan_accountacc = l_acc_type
                          AND c.fk_loan_accountacc = l_acc_sn
                          AND a.origin_id = l_fkgd_has_a_primary
                          AND a.origin_type = l_charge_type
                          AND a.charge_code = l_charge_code
                          AND d.fk_glg_h_curr_ggro = l_group
                          AND (   l_loan_stat = '0'
                               OR (    l_loan_stat = '1'
                                   AND a.accnt_status = l_loan_status))
                          AND (   l_loan_class = '0'
                               OR (    l_loan_class = '1'
                                   AND a.accnt_class = c.loan_class))
                          AND a.duration_days_to = l_durationmin)
            DO
               SET l_glg_account = c4100.fk_glg_account;
            END FOR;
            FOR c5
               AS (SELECT   get_amount_for_agr (
                               sql_statement
                              ,table_name
                              ,l_fk_unitcode
                              ,l_acc_type
                              ,l_acc_sn
                              ,l_account_number
                              ,l_charge_type
                              ,l_charge_code)
                               amount
                   FROM     class_gl_td_origin
                   WHERE        prft_system = '4'
                            AND origin_id = l_fkgd_has_a_primary)
            DO
               SET l_amount = c5.amount;
            END FOR;
            IF l_amount <> 0
            THEN
               SET l_serialnum = l_serialnum + 1;
               IF l_glg_account IS NOT NULL
               THEN
                  INSERT INTO rep_74220_det (
                                 serialnum
                                ,prft_system
                                ,gl_account
                                ,acc_move_curr
                                ,acc_unit
                                ,prof_account
                                ,gl_balance
                                ,origin_id
                                ,origin_type
                                ,charge_code)
                  VALUES      (
                                 l_serialnum
                                ,'4'
                                ,l_glg_account
                                ,l_fkcur_is_moved_in
                                ,l_monotoring_unit
                                ,l_account_number
                                ,l_amount
                                ,l_fkgd_has_a_primary
                                ,l_charge_type
                                ,NVL (l_charge_code, '0'));
               ELSE
                  INSERT INTO rep_74220_det_exp (
                                 serialnum
                                ,acc_unit
                                ,acc_move_curr
                                ,gl_account
                                ,prft_system
                                ,prof_account
                                ,origin_id
                                ,gl_balance
                                ,errormessage
                                ,origin_type
                                ,charge_code
                                ,tmstamp)
                  VALUES      (
                                 l_serialnum
                                ,l_monotoring_unit
                                ,l_fkcur_is_moved_in
                                ,' '
                                ,'4'
                                ,l_account_number
                                ,l_fkgd_has_a_primary
                                ,l_amount
                                ,'GL ACCOUNT IS NOT DEFINED!'
                                ,l_charge_type
                                ,l_charge_code
                                ,current TIMESTAMP);
               END IF;
            END IF;
         END FOR;
      END FOR;
   END FOR;
   --WE DON T WANT THE AMOUNT OF INTEREST SUSPENDED WHEN THE LOAN ACCOUNT IS IN PL STATUS
   DELETE rep_74220_det
   WHERE      origin_id IN  ('64','3','77')
          AND prof_account IN (SELECT profits_account.account_number
                               FROM   r_loan_account_inf
                                     ,profits_account
                                     ,r_loan_account
                                     ,batch_parameters w
                               WHERE      profits_account.prft_system = '4'
                                      AND r_loan_account_inf.fk_loan_accountfk =
                                             profits_account.lns_open_unit
                                      AND r_loan_account_inf.fk0loan_accountacc =
                                             profits_account.lns_type
                                      AND r_loan_account_inf.fk_loan_accountacc =
                                             profits_account.lns_sn
                                      AND profits_account.lns_open_unit =
                                             r_loan_account.fk_unitcode
                                      AND profits_account.lns_type =
                                             r_loan_account.acc_type
                                      AND profits_account.lns_sn =
                                             r_loan_account.acc_sn
                                      AND r_loan_account_inf.loan_class = '0'
                                      AND profits_account.prft_system = '4'
                                      AND w.program_id = p_program_id
                                      AND profits_account.cust_id BETWEEN w.customer_from
                                                                      AND w.customer_to);
   UPDATE rep_74220_det
   SET    gl_balance = -1 * gl_balance
   WHERE      origin_id IN ('64', '60','3','77')
          AND prof_account IN (SELECT account_number
                               FROM   profits_account z, batch_parameters w
                               WHERE      z.prft_system = '4'
                                      AND w.program_id = p_program_id
                                      AND z.cust_id BETWEEN w.customer_from
                                                        AND w.customer_to);
END;

