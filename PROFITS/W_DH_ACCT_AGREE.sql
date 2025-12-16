create table W_DH_ACCT_AGREE
(
    UPPER_ACCT_KEY   DECIMAL(11),
    LOWER_ACCT_KEY   DECIMAL(11),
    EFF_FROM_DATE    DATE,
    EFF_TO_DATE      DATE,
    ROW_CURRENT_FLAG SMALLINT,
    LEVELS_REMOVED   DECIMAL(1)
);

create unique index PK_W_DH_ACCT_AGREE
    on W_DH_ACCT_AGREE (UPPER_ACCT_KEY, LOWER_ACCT_KEY, EFF_FROM_DATE);

CREATE PROCEDURE W_DH_ACCT_AGREE ( )
  SPECIFIC SQL160620112633957
  LANGUAGE SQL
  NOT DETERMINISTIC
  EXTERNAL ACTION
  MODIFIES SQL DATA
  CALLED ON NULL INPUT
  INHERIT SPECIAL REGISTERS
  OLD SAVEPOINT LEVEL
BEGIN
UPDATE w_dh_acct_agree
SET    eff_to_date =
          UTILPKG.add_days (
             (SELECT scheduled_date FROM bank_parameters)
            ,-1)
      ,row_current_flag = 0
WHERE      row_current_flag = 1
       AND (lower_acct_key, upper_acct_key) IN (SELECT t.lower_acct_key
                                                      ,t.upper_acct_key
                                                FROM   w_dh_acct_agree t
                                                       INNER JOIN
                                                       (SELECT lower_acct_key
                                                              ,upper_acct_key
                                                              ,levels_removed
                                                        FROM   w_dh_acct_agree
                                                        WHERE  row_current_flag =
                                                                  1
                                                        MINUS
                                                        (SELECT pl.account_ser_num
                                                                   lower_acct_key
                                                               ,pa.account_ser_num
                                                                   upper_acct_key
                                                               ,DECODE (
                                                                   pa.account_ser_num
                                                                  ,pl.account_ser_num, 0
                                                                  ,1)
                                                                   levels_removed
                                                         FROM   profits_account pa
                                                                JOIN
                                                                r_agreement
                                                                   ON     pa.agr_unit =
                                                                             r_agreement.fk_unitcode
                                                                      AND pa.agr_membership_sn =
                                                                             r_agreement.agr_membership_sn
                                                                      AND pa.agr_sn =
                                                                             r_agreement.agr_sn
                                                                      AND pa.agr_year =
                                                                             r_agreement.agr_year
                                                                      AND pa.prft_system =
                                                                             19
                                                                JOIN
                                                                r_loan_account loan_account0
                                                                   ON     loan_account0.fk_agreementagr_me =
                                                                             r_agreement.agr_membership_sn
                                                                      AND loan_account0.fk_agreementagr_sn =
                                                                            r_agreement.agr_sn
                                                                      AND loan_account0.fk_agreementagr_ye =
                                                                             r_agreement.agr_year
                                                                      AND loan_account0.fk_agreementfk_uni =
                                                                             r_agreement.fk_unitcode
                                                                JOIN
                                                                profits_account pl
                                                                   ON     pl.lns_open_unit =
                                                                             loan_account0.fk_unitcode
                                                                      AND pl.lns_sn =
                                                                             loan_account0.acc_sn
                                                                      AND pl.lns_type =
                                                                             loan_account0.acc_type
                                                                      AND pl.prft_system <>
                                                                             19
                                                         UNION
                                                         SELECT pl2.account_ser_num
                                                                   lower_acct_key
                                                               ,pl2.account_ser_num
                                                                   upper_acct_key
                                                               ,0
                                                                   levels_removed
                                                         FROM   r_loan_account l2
                                                                JOIN
                                                                profits_account pl2
                                                                   ON     pl2.lns_open_unit =
                                                                             l2.fk_unitcode
                                                                      AND pl2.lns_sn =
                                                                             l2.acc_sn
                                                                      AND pl2.lns_type =
                                                                             l2.acc_type
                                                                      AND pl2.prft_system <>
                                                                             19
                                                         UNION
                                                         SELECT pa2.account_ser_num
                                                                   lower_acct_key
                                                               ,pa2.account_ser_num
                                                                   upper_acct_key
                                                               ,0
                                                                   levels_removed
                                                         FROM   profits_account pa2
                                                         WHERE  pa2.prft_system =
                                                                   19)) s
                                                          ON     s.upper_acct_key =
                                                                    s.upper_acct_key
                                                             AND s.lower_acct_key =
                                                                    t.lower_acct_key);
INSERT INTO w_dh_acct_agree (
               eff_from_date
              ,eff_to_date
              ,row_current_flag
              ,lower_acct_key
              ,upper_acct_key
              ,levels_removed)
   SELECT (SELECT scheduled_date FROM bank_parameters) eff_from_date
         ,DATE '9999-12-31' eff_to_date
         ,1 row_current_flag
         ,lower_acct_key
         ,upper_acct_key
         ,levels_removed
   FROM   ( (SELECT pl.account_ser_num lower_acct_key
                   ,pa.account_ser_num upper_acct_key
                   ,DECODE (pa.account_ser_num, pl.account_ser_num, 0, 1)
                       levels_removed
             FROM   profits_account pa
                    JOIN r_agreement
                       ON     pa.agr_unit = r_agreement.fk_unitcode
                          AND pa.agr_membership_sn =
                                 r_agreement.agr_membership_sn
                          AND pa.agr_sn = r_agreement.agr_sn
                          AND pa.agr_year = r_agreement.agr_year
                          AND pa.prft_system = 19
                    JOIN r_loan_account loan_account0
                       ON     loan_account0.fk_agreementagr_me =
                                 r_agreement.agr_membership_sn
                          AND loan_account0.fk_agreementagr_sn =
                                 r_agreement.agr_sn
                          AND loan_account0.fk_agreementagr_ye =
                                 r_agreement.agr_year
                          AND loan_account0.fk_agreementfk_uni =
                                 r_agreement.fk_unitcode
                    JOIN profits_account pl
                       ON     pl.lns_open_unit = loan_account0.fk_unitcode
                          AND pl.lns_sn = loan_account0.acc_sn
                          AND pl.lns_type = loan_account0.acc_type
                          AND pl.prft_system <> 19
             UNION
             SELECT pl2.account_ser_num lower_acct_key
                   ,pl2.account_ser_num upper_acct_key
                   ,0 levels_removed
             FROM   r_loan_account l2
                    JOIN profits_account pl2
                       ON     pl2.lns_open_unit = l2.fk_unitcode
                          AND pl2.lns_sn = l2.acc_sn
                          AND pl2.lns_type = l2.acc_type
                          AND pl2.prft_system <> 19
             UNION
             SELECT pa2.account_ser_num lower_acct_key
                   ,pa2.account_ser_num upper_acct_key
                   ,0 levels_removed
             FROM   profits_account pa2
             WHERE  pa2.prft_system = 19)
           MINUS
           SELECT lower_acct_key, upper_acct_key, levels_removed
           FROM   w_dh_acct_agree
           WHERE  row_current_flag = 1);
END;

