create table W_FACT_AUDIT
(
    TRX_DATE             DATE,
    TRX_USER_CODE        CHAR(8),
    TRX_UNIT_CODE        INTEGER,
    TRX_SN               INTEGER,
    TRX_INTERNAL_SN      SMALLINT    default 0,
    EVENT_TIMESTAMP      TIMESTAMP(6),
    ACCT_KEY             DECIMAL(11) default 0,
    OLD_ACCT_STATUS      VARCHAR(15) default 'n/a',
    NEW_ACCT_STATUS      VARCHAR(15) default 'n/a',
    NEW_EFFECTIVE_DATE   DATE,
    OLD_DB_INT_RT_SP_IND VARCHAR(4)  default 'n/a',
    NEW_DB_INT_RT_SP_IND VARCHAR(4)  default 'n/a',
    OLD_CR_INT_RT_SP_IND VARCHAR(4)  default 'n/a',
    NEW_CR_INT_RT_SP_IND VARCHAR(4)  default 'n/a',
    OLD_DB_INTR_RATE_SPR DECIMAL(8, 4),
    NEW_CR_INTR_RATE_SPR DECIMAL(8, 4),
    OLD_CR_INTR_RATE_SPR DECIMAL(8, 4),
    NEW_DB_INTR_RATE_SPR DECIMAL(8, 4)
);

create unique index PK_W_FACT_AUDIT
    on W_FACT_AUDIT (TRX_DATE, TRX_USER_CODE, TRX_UNIT_CODE, TRX_SN, TRX_INTERNAL_SN, ACCT_KEY);

CREATE PROCEDURE W_FACT_AUDIT ( )
  SPECIFIC SQL160620112637079
  LANGUAGE SQL
  NOT DETERMINISTIC
  EXTERNAL ACTION
  MODIFIES SQL DATA
  CALLED ON NULL INPUT
  INHERIT SPECIAL REGISTERS
  OLD SAVEPOINT LEVEL
BEGIN
MERGE INTO w_fact_audit a
USING      (SELECT trx_date trx_date
                  ,trx_usr trx_user_code
                  ,trx_unit trx_unit_code
                  ,trx_usr_sn trx_sn
                  ,tun_internal_sn trx_internal_sn
                  ,CAST (tmstamp AS TIMESTAMP) event_timestamp
                  ,profits_account.account_ser_num acct_key
                  ,CASE o_status
                      WHEN '0' THEN 'Deleted'
                      WHEN '1' THEN 'Active'
                      WHEN '2' THEN 'Locked'
                      WHEN '3' THEN 'Closed by Cust.'
                      WHEN '4' THEN 'Closed by Bank'
                      WHEN '5' THEN 'Blocked'
                      WHEN '6' THEN 'Dormant'
                      WHEN '7' THEN 'Unfunded'
                      WHEN '8' THEN 'Inactive'
                      ELSE 'n/a'
                   END
                      old_acct_status
                  ,CASE i_status
                      WHEN '0' THEN 'Deleted'
                      WHEN '1' THEN 'Active'
                      WHEN '2' THEN 'Locked'
                      WHEN '3' THEN 'Closed by Cust.'
                      WHEN '4' THEN 'Closed by Bank'
                      WHEN '5' THEN 'Blocked'
                      WHEN '6' THEN 'Dormant'
                      WHEN '7' THEN 'Unfunded'
                      WHEN '8' THEN 'Inactive'
                      ELSE 'n/a'
                   END
                      new_acct_status
                  ,o_effective_date new_effective_date
                  ,DECODE (
                      i_cr_int_rt_sp_ind
                     ,'0', 'Unit'
                     ,'1', 'Rate'
                     ,'n/a')
                      old_db_int_rt_sp_ind
                  ,DECODE (
                      o_cr_int_rt_sp_ind
                     ,'0', 'Unit'
                     ,'1', 'Rate'
                     ,'n/a')
                      new_db_int_rt_sp_ind
                  ,DECODE (
                      i_cr_int_rt_sp_ind
                     ,'0', 'Unit'
                     ,'1', 'Rate'
                     ,'n/a')
                      old_cr_int_rt_sp_ind
                  ,DECODE (
                      o_cr_int_rt_sp_ind
                     ,'0', 'Unit'
                     ,'1', 'Rate'
                     ,'n/a')
                      new_cr_int_rt_sp_ind
                  ,i_db_intr_rate_spr old_db_intr_rate_spr
                  ,o_cr_intr_rate_spr new_cr_intr_rate_spr
                  ,o_cr_intr_rate_spr old_cr_intr_rate_spr
                  ,i_db_intr_rate_spr new_db_intr_rate_spr
            FROM   dep_history
                   JOIN profits_account
                      ON     profits_account.dep_acc_number =
                                dep_history.account_number
                         AND profits_account.prft_system = 3) b
ON         (    a.trx_date = b.trx_date
            AND a.trx_user_code = b.trx_user_code
            AND a.trx_unit_code = b.trx_unit_code
            AND a.trx_sn = b.trx_sn
            AND a.trx_internal_sn = b.trx_internal_sn
            AND a.acct_key = b.acct_key)
WHEN NOT MATCHED
THEN
   INSERT     (
                 trx_date
                ,trx_user_code
                ,trx_unit_code
                ,trx_sn
                ,trx_internal_sn
                ,event_timestamp
                ,acct_key
                ,old_acct_status
                ,new_acct_status
                ,new_effective_date
                ,old_db_int_rt_sp_ind
                ,new_db_int_rt_sp_ind
                ,old_cr_int_rt_sp_ind
                ,new_cr_int_rt_sp_ind
                ,old_db_intr_rate_spr
                ,new_cr_intr_rate_spr
                ,old_cr_intr_rate_spr
                ,new_db_intr_rate_spr)
   VALUES     (
                 b.trx_date
                ,b.trx_user_code
                ,b.trx_unit_code
                ,b.trx_sn
                ,b.trx_internal_sn
                ,b.event_timestamp
                ,b.acct_key
                ,b.old_acct_status
                ,b.new_acct_status
                ,b.new_effective_date
                ,b.old_db_int_rt_sp_ind
                ,b.new_db_int_rt_sp_ind
                ,b.old_cr_int_rt_sp_ind
                ,b.new_cr_int_rt_sp_ind
                ,b.old_db_intr_rate_spr
                ,b.new_cr_intr_rate_spr
                ,b.old_cr_intr_rate_spr
                ,b.new_db_intr_rate_spr);
END;

