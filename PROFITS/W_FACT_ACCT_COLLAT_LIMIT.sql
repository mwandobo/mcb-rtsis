create table W_FACT_ACCT_COLLAT_LIMIT
(
    EFF_FROM_DATE          DATE,
    EFF_TO_DATE            DATE,
    ACCT_KEY               DECIMAL(11),
    COLLAT_COMBO_KEY       CHAR(20),
    CUST_ID                INTEGER  not null,
    LIMIT_INTERNAL_SN      SMALLINT not null,
    COLLAT_PRODUCT_ID      INTEGER  not null,
    COLLAT_UNIT_CODE       INTEGER  not null,
    ENTRY_STATUS_IND       CHAR(7),
    INSERTION_DT           DATE,
    EXPIRY_DT              DATE,
    EST_VALUE_AMN          DECIMAL(15, 2),
    ROW_CURRENT_FLAG       SMALLINT default 0,
    YIELD_LIMIT_AMN        DECIMAL(15, 2),
    REFERENCE_NUMBER       CHAR(20),
    PROFITS_SYSTEM         DECIMAL(22),
    ACCOUNT_NO             VARCHAR(50),
    COLLATERAL_FIXING_RATE DECIMAL(12, 6),
    FINSC_DESCRIPTION      VARCHAR(40)
);

create unique index PK_W_FACT_ACCT_COLLAT_LIMIT
    on W_FACT_ACCT_COLLAT_LIMIT (EFF_FROM_DATE, EFF_TO_DATE, CUST_ID, COLLAT_COMBO_KEY, ACCT_KEY, LIMIT_INTERNAL_SN);

CREATE PROCEDURE W_FACT_ACCT_COLLAT_LIMIT ( )
  SPECIFIC SQL160620112636573
  LANGUAGE SQL
  NOT DETERMINISTIC
  EXTERNAL ACTION
  MODIFIES SQL DATA
  CALLED ON NULL INPUT
  INHERIT SPECIAL REGISTERS
  OLD SAVEPOINT LEVEL
BEGIN
UPDATE w_fact_acct_collat_limit
SET    eff_to_date =
          UTILPKG.add_days (
             (SELECT scheduled_date FROM bank_parameters)
            ,-1)
      ,row_current_flag = 0
WHERE      row_current_flag = 1
       AND (acct_key
           ,collat_combo_key
           ,cust_id
           ,limit_internal_sn
           ,collat_product_id
           ,collat_unit_code
           ,entry_status_ind
           ,insertion_dt
           ,expiry_dt
           ,est_value_amn
           ,yield_limit_amn
           ,reference_number
           ,account_no
           ,profits_system) IN (SELECT acct_key
                                      ,collat_combo_key
                                      ,cust_id
                                      ,limit_internal_sn
                                      ,collat_product_id
                                      ,collat_unit_code
                                      ,entry_status_ind
                                      ,insertion_dt
                                      ,expiry_dt
                                      ,est_value_amn
                                      ,yield_limit_amn
                                      ,reference_number
                                      ,account_no
                                      ,profits_system
                                FROM   w_fact_acct_collat_limit
                                WHERE  row_current_flag = 1
                                MINUS
                                SELECT account_ser_num acct_key
                                      ,CAST (
                                             fk_collateralfk_co
                                          || '|'
                                          || fk_collateralfk_un
                                          || '|'
                                          || fk_collateralcolla AS CHAR (20))
                                          collat_combo_key
                                      ,fk_customercust_id cust_id
                                      ,internal_sn limit_internal_sn
                                      ,fk_collateralfk_co collat_product_id
                                      ,fk_collateralfk_un collat_unit_code
                                      ,CAST (
                                          CASE ac.entry_status
                                             WHEN '1' THEN 'Active'
                                             WHEN '0' THEN 'Deleted'
                                             ELSE 'n/a'
                                          END AS CHAR (7))
                                          entry_status_ind
                                      ,ac.insertion_dt
                                      ,ac.expiry_dt
                                      ,est_value_amn
                                      ,yield_limit_amn
                                      ,ac.reference_number
                                      ,   TRIM (ac.prft_account)
                                       || '-'
                                       || ac.account_cd
                                          account_no
                                      ,ac.profits_system profits_system
                                FROM   r_account_collater ac
                                       INNER JOIN profits_account
                                          ON     prft_account =
                                                    account_number
                                             AND prft_system = profits_system);
INSERT INTO w_fact_acct_collat_limit (
               eff_from_date
              ,eff_to_date
              ,acct_key
              ,collat_combo_key
              ,cust_id
              ,limit_internal_sn
              ,collat_product_id
              ,collat_unit_code
              ,entry_status_ind
              ,insertion_dt
              ,expiry_dt
              ,est_value_amn
              ,yield_limit_amn
              ,reference_number
              ,row_current_flag
              ,account_no
              ,profits_system)
   SELECT (SELECT scheduled_date FROM bank_parameters) eff_from_date
         ,DATE '9999-12-31' eff_to_date
         ,acct_key
         ,collat_combo_key
         ,cust_id
         ,limit_internal_sn
         ,collat_product_id
         ,collat_unit_code
         ,entry_status_ind
         ,insertion_dt
         ,expiry_dt
         ,est_value_amn
         ,yield_limit_amn
         ,reference_number
         ,1 row_current_flag
         ,account_no
         ,profits_system
   FROM   (SELECT account_ser_num acct_key
                 ,CAST (
                        fk_collateralfk_co
                     || '|'
                     || fk_collateralfk_un
                     || '|'
                     || fk_collateralcolla AS CHAR (20))
                     collat_combo_key
                 ,fk_customercust_id cust_id
                 ,internal_sn limit_internal_sn
                 ,fk_collateralfk_co collat_product_id
                 ,fk_collateralfk_un collat_unit_code
                 ,CAST (
                     CASE ac.entry_status
                        WHEN '1' THEN 'Active'
                        WHEN '0' THEN 'Deleted'
                        ELSE 'n/a'
                     END AS CHAR (7))
                     entry_status_ind
                 ,ac.insertion_dt
                 ,ac.expiry_dt
                 ,est_value_amn
                 ,yield_limit_amn
                 ,reference_number
                 ,TRIM (ac.prft_account) || '-' || ac.account_cd account_no
                 ,profits_system
           FROM   r_account_collater ac
                  INNER JOIN profits_account
                     ON     prft_account = account_number
                        AND prft_system = profits_system
           MINUS
           SELECT acct_key
                 ,collat_combo_key
                 ,cust_id
                 ,limit_internal_sn
                 ,collat_product_id
                 ,collat_unit_code
                 ,entry_status_ind
                 ,insertion_dt
                 ,expiry_dt
                 ,est_value_amn
                 ,yield_limit_amn
                 ,reference_number
                 ,account_no
                 ,profits_system
           FROM   w_fact_acct_collat_limit
           WHERE  row_current_flag = 1);
END;

