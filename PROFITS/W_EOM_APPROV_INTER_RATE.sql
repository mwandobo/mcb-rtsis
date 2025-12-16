create table W_EOM_APPROV_INTER_RATE
(
    EOM_DATE           DATE,
    DEALER_USER_CODE   CHAR(8),
    DEALER_REF_NO      CHAR(20),
    ACCT_KEY           DECIMAL(11),
    C_DIGIT            SMALLINT,
    TRX_UNIT           INTEGER,
    ID_CURRENCY        INTEGER,
    DB_INTEREST_RATE   DECIMAL(8, 4),
    CR_INTEREST_RATE   DECIMAL(8, 4),
    TRX_USR_SN         INTEGER,
    ACCOUNT_NUMBER     DECIMAL(11),
    RATE_AMOUNT        DECIMAL(15, 2),
    TRX_DATE           DATE,
    START_DATE         DATE,
    TIMESTMP           DATE,
    STATUS_FLAG        CHAR(1),
    RENEWAL_FLAG       CHAR(1),
    TRX_USR            CHAR(8),
    REQUEST_USER_CODE  CHAR(8),
    END_DATE           DATE,
    TIME_DEP_TRANS_FLG CHAR(1),
    INTER_PAY_OPTIONS  CHAR(1),
    DAYS_DURATION      SMALLINT,
    LOAN_ACC_CD        SMALLINT,
    LOAN_ACC_NUMBER    VARCHAR(40),
    PRFT_SYSTEM        SMALLINT
);

create unique index PK_EOM_APPROV_INTER_RATE
    on W_EOM_APPROV_INTER_RATE (EOM_DATE, DEALER_USER_CODE, DEALER_REF_NO);

CREATE PROCEDURE W_EOM_APPROV_INTER_RATE ( )
  SPECIFIC SQL160620112636674
  LANGUAGE SQL
  NOT DETERMINISTIC
  EXTERNAL ACTION
  MODIFIES SQL DATA
  CALLED ON NULL INPUT
  INHERIT SPECIAL REGISTERS
  OLD SAVEPOINT LEVEL
BEGIN
DELETE w_eom_approv_inter_rate
WHERE  eom_date = (SELECT scheduled_date FROM bank_parameters);
MERGE INTO w_eom_approv_inter_rate a
USING      (SELECT (SELECT scheduled_date FROM bank_parameters) eom_date
                  ,air.fk_usrcode dealer_user_code
                  ,air.dealer_ref_no
                  ,profits_account.account_ser_num acct_key
                  ,air.c_digit
                  ,air.trx_unit
                  ,air.id_currency
                  ,air.db_interest_rate
                  ,air.cr_interest_rate
                  ,air.trx_usr_sn
                  ,air.account_number
                  ,air.rate_amount
                  ,air.trx_date
                  ,air.start_date
                  ,air.timestmp
                  ,air.status_flag
                  ,air.renewal_flag
                  ,air.trx_usr
                  ,air.fk0usrcode request_user_code
                  ,air.end_date
                  ,air.time_dep_trans_flg
                  ,air.inter_pay_options
                  ,air.days_duration
                  ,air.loan_acc_cd
                  ,air.loan_acc_number
                  ,air.prft_system
            FROM   approv_inter_rate air
                   LEFT JOIN profits_account
                      ON     profits_account.dep_acc_number =
                                air.account_number
                         AND profits_account.prft_system = 3
                         AND profits_account.dep_acc_number <> 0) b
ON         (    a.eom_date = b.eom_date
            AND a.dealer_user_code = b.dealer_user_code
            AND a.dealer_ref_no = b.dealer_ref_no)
WHEN NOT MATCHED
THEN
   INSERT     (
                 eom_date
                ,dealer_user_code
                ,dealer_ref_no
                ,acct_key
                ,c_digit
                ,trx_unit
                ,id_currency
                ,db_interest_rate
                ,cr_interest_rate
                ,trx_usr_sn
                ,account_number
                ,rate_amount
                ,trx_date
                ,start_date
                ,timestmp
                ,status_flag
                ,renewal_flag
                ,trx_usr
                ,request_user_code
                ,end_date
                ,time_dep_trans_flg
                ,inter_pay_options
                ,days_duration
                ,loan_acc_cd
                ,loan_acc_number
                ,prft_system)
   VALUES     (
                 b.eom_date
                ,b.dealer_user_code
                ,b.dealer_ref_no
                ,b.acct_key
                ,b.c_digit
                ,b.trx_unit
                ,b.id_currency
                ,b.db_interest_rate
                ,b.cr_interest_rate
                ,b.trx_usr_sn
                ,b.account_number
                ,b.rate_amount
                ,b.trx_date
                ,b.start_date
                ,b.timestmp
                ,b.status_flag
                ,b.renewal_flag
                ,b.trx_usr
                ,b.request_user_code
                ,b.end_date
                ,b.time_dep_trans_flg
                ,b.inter_pay_options
                ,b.days_duration
                ,b.loan_acc_cd
                ,b.loan_acc_number
                ,b.prft_system);
END;

