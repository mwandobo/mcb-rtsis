create table W_DIM_ACCOUNT
(
    ACCT_KEY       DECIMAL(10),
    ACCOUNT_NUMBER CHAR(40),
    PRFT_SYSTEM    SMALLINT,
    ACCOUNT_CD     SMALLINT,
    ACCOUNT_NO     VARCHAR(43),
    DEP_ACC_NUMBER DECIMAL(11),
    LNS_OPEN_UNIT  INTEGER,
    LNS_TYPE       SMALLINT,
    LNS_SN         INTEGER,
    LG_ACC_SN      DECIMAL(13)
);

create unique index IDX2_W_DIM_ACCOUNT
    on W_DIM_ACCOUNT (DEP_ACC_NUMBER);

create unique index IDX_W_DIM_ACCOUNT
    on W_DIM_ACCOUNT (LNS_OPEN_UNIT, LNS_TYPE, LNS_SN);

create unique index PK_DIMACCOUNT
    on W_DIM_ACCOUNT (ACCT_KEY);

create unique index UIX_W_DIM_ACCOUNT
    on W_DIM_ACCOUNT (ACCOUNT_NUMBER, PRFT_SYSTEM);

CREATE PROCEDURE W_DIM_ACCOUNT ( )
  SPECIFIC SQL160620112633449
  LANGUAGE SQL
  NOT DETERMINISTIC
  EXTERNAL ACTION
  MODIFIES SQL DATA
  CALLED ON NULL INPUT
  INHERIT SPECIAL REGISTERS
  OLD SAVEPOINT LEVEL
BEGIN
MERGE INTO w_dim_account b
USING      (SELECT account_ser_num
                  ,account_number
                  ,prft_system
                  ,account_cd
                  ,DECODE (
                      LENGTH (TRIM (account_number))
                     ,NULL, ' '
                     ,TRIM (account_number) || '-' || account_cd)
                      account_no
                  ,DECODE (
                      profits_account.prft_system
                     ,3, profits_account.dep_acc_number
                     ,-6)
                      dep_acc_number
                  ,DECODE (
                      profits_account.prft_system
                     ,4, profits_account.lns_open_unit
                     ,-6)
                      lns_open_unit
                  ,DECODE (
                      profits_account.prft_system
                     ,4, profits_account.lns_type
                     ,-6)
                      lns_type
                  ,DECODE (
                      profits_account.prft_system
                     ,4, profits_account.lns_sn
                     ,-6)
                      lns_sn
                  ,NVL (
                      CASE
                         WHEN profits_account.lg_acc_sn > 0
                         THEN
                            profits_account.lg_acc_sn
                      END
                     ,-6)
                      lg_acc_sn
            FROM   profits_account
            WHERE  NVL (account_ser_num, 0) <> 0) e
ON         (    b.account_number = e.account_number
            AND b.prft_system = e.prft_system)
WHEN NOT MATCHED
THEN
   INSERT     (
                 acct_key
                ,account_number
                ,prft_system
                ,account_cd
                ,account_no
                ,dep_acc_number
                ,lns_open_unit
                ,lns_type
                ,lns_sn
                ,lg_acc_sn)
   VALUES     (
                 e.account_ser_num
                ,e.account_number
                ,e.prft_system
                ,e.account_cd
                ,e.account_no
                ,e.dep_acc_number
                ,e.lns_open_unit
                ,e.lns_type
                ,e.lns_sn
                ,e.lg_acc_sn);
END;

