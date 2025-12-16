create table W_DH_CUSTOMER_AGREEMENT
(
    ACCT_KEY              DECIMAL(11) not null,
    CUST_ID               DECIMAL(7)  not null,
    ROW_EFFECTIVE_DATE    DATE        not null,
    ROW_EXPIRATION_DATE   DATE,
    ROW_CURRENT_FLAG      DECIMAL(5),
    AGREEMENT_SEQUENCE_NO DECIMAL(5),
    MAIN_BENEFICIARY_FLAG VARCHAR(16),
    STATUS_FLAG           VARCHAR(8),
    CUSTOMER_CD           DECIMAL(1),
    constraint PK_W_DH_CUSTOMER_AGREEMENT
        primary key (ACCT_KEY, CUST_ID, ROW_EFFECTIVE_DATE)
);

CREATE PROCEDURE W_DH_CUSTOMER_AGREEMENT ( )
  SPECIFIC SQL160620112633754
  LANGUAGE SQL
  NOT DETERMINISTIC
  EXTERNAL ACTION
  MODIFIES SQL DATA
  CALLED ON NULL INPUT
  INHERIT SPECIAL REGISTERS
  OLD SAVEPOINT LEVEL
BEGIN
UPDATE w_dh_customer_agreement
SET    row_expiration_date =
          UTILPKG.add_days (
             (SELECT scheduled_date FROM bank_parameters)
            ,-1)
      ,row_current_flag = 0
WHERE      row_current_flag = 1
       AND (acct_key, cust_id) IN (SELECT t.acct_key, t.cust_id
                                   FROM   w_dh_customer_agreement t
                                          INNER JOIN
                                          (SELECT acct_key
                                                 ,cust_id
                                                 ,agreement_sequence_no
                                                 ,main_beneficiary_flag
                                                 ,status_flag
                                                 ,customer_cd
                                           FROM   w_dh_customer_agreement
                                           WHERE  row_current_flag = 1
                                           MINUS
                                           SELECT   b.account_ser_num acct_key
                                                   ,d.fk_customercust_id
                                                       cust_id
                                                   ,MAX (fk_agreementagr_sn)
                                                       agreement_sequence_no
                                                   ,MAX (
                                                       DECODE (
                                                          main_benef_flg
                                                         ,'1', 'Main Beneficiary'
                                                         ,'Co-beneficiary'))
                                                       main_beneficiary_flag
                                                   ,'Active' status_flag
                                                   ,MAX (cust.c_digit)
                                                       customer_cd
                                           FROM     profits_account b
                                                   ,r_agreement c
                                                   ,agreement_benef d
                                                   ,customer cust
                                           WHERE        b.agr_unit =
                                                           c.fk_unitcode
                                                    AND b.agr_year = c.agr_year
                                                    AND b.agr_sn = c.agr_sn
                                                    AND b.agr_membership_sn =
                                                           c.agr_membership_sn
                                                    AND b.prft_system = 19
                                                    AND c.fk_unitcode =
                                                           d.fk_agreementfk_uni
                                                    AND c.agr_year =
                                                           d.fk_agreementagr_ye
                                                    AND c.agr_sn =
                                                           d.fk_agreementagr_sn
                                                    AND c.agr_membership_sn =
                                                           d.fk_agreementagr_me
                                                    AND cust.cust_id =
                                                           d.fk_customercust_id
                                                    AND c.agr_status IN ('2'
                                                                        ,'3')
                                                    AND entry_status = '1'
                                           GROUP BY b.account_ser_num
                                                   ,d.fk_customercust_id) s
                                             ON (    t.acct_key = s.acct_key
                                                 AND t.cust_id = s.cust_id));
INSERT INTO w_dh_customer_agreement (
               acct_key
              ,cust_id
              ,row_effective_date
              ,row_expiration_date
              ,row_current_flag
              ,agreement_sequence_no
              ,main_beneficiary_flag
              ,status_flag
              ,customer_cd)
   SELECT acct_key
         ,cust_id
         , (SELECT scheduled_date FROM bank_parameters) row_effective_date
         ,DATE '9999-12-31' row_expiration_date
         ,1 row_current_flag
         ,agreement_sequence_no
         ,main_beneficiary_flag
         ,status_flag
         ,customer_cd
   FROM   (SELECT   b.account_ser_num acct_key
                   ,d.fk_customercust_id cust_id
                   ,MAX (fk_agreementagr_sn) agreement_sequence_no
                   ,MAX (
                       DECODE (
                          main_benef_flg
                         ,'1', 'Main Beneficiary'
                         ,'Co-beneficiary'))
                       main_beneficiary_flag
                   ,'Active' status_flag
                   ,MAX (cust.c_digit) customer_cd
           FROM     profits_account b
                   ,r_agreement c
                   ,agreement_benef d
                   ,customer cust
           WHERE        b.agr_unit = c.fk_unitcode
                    AND b.agr_year = c.agr_year
                    AND b.agr_sn = c.agr_sn
                    AND b.agr_membership_sn = c.agr_membership_sn
                    AND b.prft_system = 19
                    AND c.fk_unitcode = d.fk_agreementfk_uni
                    AND c.agr_year = d.fk_agreementagr_ye
                    AND c.agr_sn = d.fk_agreementagr_sn
                    AND c.agr_membership_sn = d.fk_agreementagr_me
                    AND cust.cust_id = d.fk_customercust_id
                    AND c.agr_status IN ('2', '3')
                    AND entry_status = '1'
           GROUP BY b.account_ser_num, d.fk_customercust_id
           MINUS
           SELECT acct_key
                 ,cust_id
                 ,agreement_sequence_no
                 ,main_beneficiary_flag
                 ,status_flag
                 ,customer_cd
           FROM   w_dh_customer_agreement
           WHERE  row_current_flag = 1);
END;

