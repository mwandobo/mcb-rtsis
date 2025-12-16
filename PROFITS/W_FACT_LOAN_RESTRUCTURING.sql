create table W_FACT_LOAN_RESTRUCTURING
(
    ACCT_KEY              DECIMAL(11),
    TIME_STAMP            TIMESTAMP(6),
    TRANSACTION_TYPE      VARCHAR(22),
    TRANSACTION_DATE      DATE,
    ACCOUNT_CURRENCY_CODE VARCHAR(5),
    RM_CODE               VARCHAR(8),
    RM_NAME               VARCHAR(41),
    INSTALLMENT_AMOUNT    DECIMAL(15),
    CAPITALIZED_AMOUNT    DECIMAL(15),
    INSTALLMENT_NEXT_DATE DATE,
    ROW_TIMESTAMP         TIMESTAMP(6) default CURRENT TIMESTAMP
);

create unique index PK_W_FACT_LOAN_DURATION
    on W_FACT_LOAN_RESTRUCTURING (ACCT_KEY, TIME_STAMP);

CREATE PROCEDURE W_FACT_LOAN_RESTRUCTURING ( )
  SPECIFIC SQL160620112708388
  LANGUAGE SQL
  NOT DETERMINISTIC
  EXTERNAL ACTION
  MODIFIES SQL DATA
  CALLED ON NULL INPUT
  INHERIT SPECIAL REGISTERS
  OLD SAVEPOINT LEVEL
BEGIN
MERGE INTO w_fact_loan_restructuring a
USING      (SELECT t.acct_key
                  ,t.time_stamp
                  ,t.transaction_type
                  ,t.transaction_date
                  ,t.account_currency_code
                  ,t.rm_code
                  ,t.rm_name
                  ,t.installment_amount
                  ,t.capitalized_amount
                  ,t.installment_next_date
                  ,UTILPKG.gettimestamp () row_timestamp
            FROM   (SELECT c.account_ser_num acct_key
                          ,a.tmstamp time_stamp
                          ,'Arrears Capitalization' transaction_type
                          ,a.trx_date transaction_date
                          ,e.short_descr account_currency_code
                          ,d.fk_cust_bankempid rm_code
                          ,h.first_name || ' ' || h.last_name rm_name
                          ,CASE
                              WHEN acc_mechanism NOT IN (4, 5)
                              THEN
                                 0
                              ELSE
                                 CASE
                                    WHEN     lbdin.serial_num > 0
                                         AND b.install_count <> 0
                                    THEN
                                         (  b.install_skip_cap
                                          + b.install_skip_int)
                                       / b.install_count
                                    ELSE
                                         i.install_fixed_amn * -1
                                       + NVL (y.ledger_fee, 0)
                                       + NVL (t.insurance_amount, 0)
                                 END
                           END
                              installment_amount
                          ,a.capital_amn * -1 capitalized_amount
                          ,b.install_next_dt installment_next_date
                    FROM   bank_parameters bp
                           JOIN loan_trx_recording a ON (1 = 1)
                           INNER JOIN r_loan_account b
                              ON     b.acc_sn = a.acc_sn
                                 AND b.acc_type = a.acc_type
                                 AND b.fk_unitcode = a.acc_unit
                                 AND trx_internal_sn = 1
                           INNER JOIN profits_account c
                              ON     b.acc_sn = c.lns_sn
                                 AND b.acc_type = c.lns_type
                                 AND b.fk_unitcode = c.lns_open_unit
                           INNER JOIN currency e
                              ON c.movement_currency = e.id_currency
                           INNER JOIN loan_account_info i
                              ON     c.lns_open_unit = i.fk_loan_accountfk
                                 AND c.lns_type = i.fk0loan_accountacc
                                 AND c.lns_sn = i.fk_loan_accountacc
                           LEFT JOIN customer d ON d.cust_id = b.cust_id
                           LEFT JOIN bankemployee h
                              ON h.id = d.fk_cust_bankempid
                           LEFT JOIN w_eom_fixing_rate fr
                              ON (    fr.eom_date = bp.scheduled_date
                                  AND fr.currency_id = b.fkcur_is_moved_in)
                           LEFT JOIN w_stg_loan_insurance t
                              ON t.account_number = c.account_number
                           LEFT JOIN w_stg_loan_ledger_fee y
                              ON y.account_number = c.account_number
                           LEFT JOIN generic_detail lbdin
                              ON     lbdin.fk_generic_headpar = 'LBDIN'
                                 AND lbdin.serial_num = c.product_id
                    WHERE      c.prft_system = 4
                           AND reversal_flg != '1'
                           AND trx_code = 4201
                           AND i_justification = 44122
                           AND a.trx_date =
                                  (SELECT scheduled_date FROM bank_parameters)
                           AND b.acc_status = '1'
                    UNION ALL
                    SELECT c.account_ser_num acct_key
                          ,ldc.tmstamp time_stamp
                          ,'Rescheduling' transaction_type
                          ,ldc.trx_date transaction_date
                          ,e.short_descr account_currency_code
                          ,d.fk_cust_bankempid rm_code
                          ,h.first_name || ' ' || h.last_name rm_name
                          ,CASE
                              WHEN acc_mechanism NOT IN (4, 5)
                              THEN
                                 0
                              ELSE
                                 CASE
                                    WHEN     lbdin.serial_num > 0
                                         AND b.install_count <> 0
                                    THEN
                                         (  b.install_skip_cap
                                          + b.install_skip_int)
                                       / b.install_count
                                    ELSE
                                         i.install_fixed_amn * -1
                                       + NVL (y.ledger_fee, 0)
                                       + NVL (t.insurance_amount, 0)
                                 END
                           END
                              installment_amount
                          ,0 capitalized_amount
                          ,b.install_next_dt installment_next_date
                    FROM   bank_parameters bp
                           INNER JOIN r_loan_account b ON (1 = 1)
                           INNER JOIN profits_account c
                              ON     b.acc_sn = c.lns_sn
                                 AND b.acc_type = c.lns_type
                                 AND b.fk_unitcode = c.lns_open_unit
                                 AND c.prft_system = 4
                           INNER JOIN loan_duration_chg ldc
                              ON     b.acc_sn = ldc.lns_sn
                                 AND b.acc_type = ldc.lns_type
                                 AND b.fk_unitcode = ldc.lns_open_unit
                           LEFT JOIN loan_account_info i
                              ON     c.lns_open_unit = i.fk_loan_accountfk
                                 AND c.lns_type = i.fk0loan_accountacc
                                 AND c.lns_sn = i.fk_loan_accountacc
                           LEFT JOIN customer d ON d.cust_id = b.cust_id
                           LEFT JOIN currency e
                              ON c.movement_currency = e.id_currency
                           LEFT JOIN bankemployee h
                              ON h.id = d.fk_cust_bankempid
                          LEFT JOIN w_eom_fixing_rate fr
                              ON (    fr.eom_date = bp.scheduled_date
                                  AND fr.currency_id = b.fkcur_is_moved_in)
                           LEFT JOIN w_stg_loan_insurance t
                              ON t.account_number = c.account_number
                           LEFT JOIN w_stg_loan_ledger_fee y
                              ON y.account_number = c.account_number
                           LEFT JOIN generic_detail lbdin
                              ON     lbdin.fk_generic_headpar = 'LBDIN'
                                 AND lbdin.serial_num = c.product_id
                    WHERE  b.acc_status = '1') t) b
ON         (a.acct_key = b.acct_key AND a.time_stamp = b.time_stamp)
WHEN NOT MATCHED
THEN
   INSERT     (
                 acct_key
                ,time_stamp
                ,transaction_type
                ,transaction_date
                ,account_currency_code
                ,rm_code
                ,rm_name
                ,installment_amount
                ,capitalized_amount
                ,installment_next_date
                ,row_timestamp)
   VALUES     (
                 b.acct_key
                ,b.time_stamp
                ,b.transaction_type
                ,b.transaction_date
                ,b.account_currency_code
                ,b.rm_code
                ,b.rm_name
                ,b.installment_amount
                ,b.capitalized_amount
                ,b.installment_next_date
                ,b.row_timestamp)
WHEN MATCHED
THEN
   UPDATE SET a.transaction_type = b.transaction_type
             ,a.transaction_date = b.transaction_date
             ,a.account_currency_code = b.account_currency_code
             ,a.rm_code = b.rm_code
             ,a.rm_name = b.rm_name
             ,a.installment_amount = b.installment_amount
             ,a.capitalized_amount = b.capitalized_amount
             ,a.installment_next_date = b.installment_next_date
             ,a.row_timestamp = b.row_timestamp;
END;

