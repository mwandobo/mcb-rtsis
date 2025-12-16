create table EOM_ACCOUNT_COLLATERAL
(
    FK_CUSTOMERCUST_ID     INTEGER     not null,
    PRFT_ACCOUNT           CHAR(40)    not null,
    EOM_DATE               DATE        not null,
    FK_COLLATERALFK_UN     INTEGER     not null,
    FK_COLLATERALFK_CO     INTEGER     not null,
    FK_COLLATERALCOLLA     DECIMAL(10) not null,
    INTERNAL_SN            SMALLINT    not null,
    ACCOUNT_NUMBER         CHAR(40)    not null,
    ACCOUNT_CD             SMALLINT,
    REVAL_CURR_FIX_RAT     DECIMAL(12, 6),
    REVAL_INIT_FIX_RAT     DECIMAL(12, 6),
    REVAL_INIT_SV          DECIMAL(15, 2),
    REVALUATION_CURRID     INTEGER,
    YIELD_UTILISED_AMN     DECIMAL(15, 2),
    YIELD_LIMIT_AMN        DECIMAL(15, 2),
    YIELD_PERC             DECIMAL(8, 4),
    CB_INSURANCE_AMN       DECIMAL(15, 2),
    UNIT_CODE              INTEGER,
    ISSUER_ACCOUNT         DECIMAL(13),
    BANK_CODE              SMALLINT,
    ITEMS_CNT              INTEGER,
    PROFITS_SYSTEM         INTEGER,
    EST_VALUE_AMN          DECIMAL(15, 2),
    EST_INSURANCE_AMN      DECIMAL(15, 2),
    FK_CURRENCYID_CURR     SMALLINT,
    NRM_BALANCE            DECIMAL(15, 2),
    OV_BALANCE             DECIMAL(15, 2),
    LG_AMOUNT_BAL          DECIMAL(15, 2),
    DEFAULT_AMN            DECIMAL(15, 2),
    TOT_EST_VALUE_AMN      DECIMAL(15, 2),
    INSERTION_DT           DATE,
    EXPIRY_DT              DATE,
    TMSTAMP                TIMESTAMP(6),
    NOTE_STATUS            CHAR(1),
    ENTRY_STATUS           CHAR(1),
    DEP_ACC_IND            CHAR(1),
    USER_CODE              CHAR(8),
    AFM_NO                 CHAR(20),
    REFERENCE_NUMBER       CHAR(20),
    FIRST_NAME             VARCHAR(20),
    ADDRESS                VARCHAR(40),
    SURNAME                VARCHAR(70),
    FK_COLLATERAL_TFK      INTEGER,
    REVAL_CHRG_AMN         DECIMAL(15, 2),
    MONITORING_UNIT        INTEGER,
    COLLATERAL_SN          DECIMAL(10),
    COLLATERAL_FK_UNITCODE INTEGER,
    COLLATERAL_STATUS      CHAR(1),
    ACCT_KEY               DECIMAL(11),
    constraint IXU_EOM_001
        primary key (FK_CUSTOMERCUST_ID, PRFT_ACCOUNT, EOM_DATE, FK_COLLATERALFK_UN, FK_COLLATERALFK_CO,
                     FK_COLLATERALCOLLA, INTERNAL_SN)
);

create unique index IDX_EOM_ACCTCOLLATERAL_ACCTKEY
    on EOM_ACCOUNT_COLLATERAL (EOM_DATE, FK_COLLATERALFK_UN, FK_COLLATERALFK_CO, FK_COLLATERALCOLLA, ACCT_KEY);

create unique index PK_EOM_ACC_COL
    on EOM_ACCOUNT_COLLATERAL (INTERNAL_SN, FK_CUSTOMERCUST_ID, FK_COLLATERALFK_CO, FK_COLLATERALFK_UN,
                               FK_COLLATERALCOLLA, PRFT_ACCOUNT, EOM_DATE);

CREATE PROCEDURE EOM_ACCOUNT_COLLATERAL ()
  SPECIFIC SQL160620112634160
  LANGUAGE SQL
  NOT DETERMINISTIC
  EXTERNAL ACTION
  MODIFIES SQL DATA
  CALLED ON NULL INPUT
  INHERIT SPECIAL REGISTERS
  OLD SAVEPOINT LEVEL
BEGIN
DELETE eom_account_collateral
WHERE  eom_date = (SELECT scheduled_date FROM bank_parameters);
INSERT INTO eom_account_collateral (
               account_number
              ,account_cd
              ,reval_curr_fix_rat
              ,reval_init_fix_rat
              ,reval_init_sv
              ,revaluation_currid
              ,yield_utilised_amn
              ,yield_limit_amn
              ,yield_perc
              ,cb_insurance_amn
              ,address
              ,afm_no
              ,dep_acc_ind
              ,unit_code
              ,note_status
              ,issuer_account
              ,bank_code
              ,internal_sn
              ,items_cnt
              ,prft_account
              ,insertion_dt
              ,entry_status
              ,profits_system
              ,reference_number
              ,est_value_amn
              ,est_insurance_amn
              ,user_code
              ,expiry_dt
              ,tmstamp
              ,first_name
              ,surname
              ,fk_customercust_id
              ,fk_collateralfk_co
              ,fk_collateralfk_un
              ,fk_collateralcolla
              ,fk_collateral_tfk
              ,fk_currencyid_curr
              ,eom_date
              ,nrm_balance
              ,ov_balance
              ,lg_amount_bal
              ,default_amn
              ,tot_est_value_amn
              ,reval_chrg_amn
              ,monitoring_unit
              ,collateral_fk_unitcode
              ,collateral_sn
              ,collateral_status
              ,acct_key)
   SELECT b.account_number
         ,b.account_cd
         ,c.reval_curr_fix_rat
         ,c.reval_init_fix_rat
         ,c.reval_init_sv
         ,c.revaluation_currid
         ,c.yield_utilised_amn
         ,c.yield_limit_amn
         ,c.yield_perc
         ,c.cb_insurance_amn
         ,address
         ,afm_no
         ,dep_acc_ind
         ,unit_code
         ,note_status
         ,issuer_account
         ,bank_code
         ,internal_sn
         ,c.items_cnt
         ,prft_account
         ,c.insertion_dt
         ,entry_status
         ,profits_system
         ,reference_number
         ,est_value_amn
         ,est_insurance_amn
         ,c.user_code
         ,expiry_dt
         ,c.tmstamp
         ,first_name
         ,surname
         ,fk_customercust_id
         ,fk_collateralfk_co
         ,fk_collateralfk_un
         ,fk_collateralcolla
         ,d.fk_collateral_tfk
         ,d.fk_currencyid_curr AS id_currency
         , (SELECT scheduled_date FROM bank_parameters) AS eom_date
         ,0 AS nrm_balance
         ,0 AS ov_balance
         ,0 AS lg_amount_bal
         ,0 AS default_amn
         ,d.tot_est_value_amn
         ,d.reval_chrg_amn
         ,d.monitoring_unit
         ,d.fk_unitcode collateral_fk_unitcode
         ,d.collateral_sn
         ,collateral_status
         ,b.account_ser_num
   FROM   profits_account b, r_account_collater c, r_collateral d
   WHERE      b.account_number = c.prft_account
          AND b.prft_system = c.profits_system
          AND c.fk_collateralfk_un = d.fk_unitcode
          AND c.fk_collateralfk_co = d.fk_collateral_tfk
          AND c.fk_collateralcolla = d.collateral_sn
          AND c.entry_status = '1';
END;

