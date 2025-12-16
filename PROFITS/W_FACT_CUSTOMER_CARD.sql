create table W_FACT_CUSTOMER_CARD
(
    CARD_NO                  VARCHAR(16) not null,
    ENTRY_STATUS_IND         VARCHAR(1)  not null,
    ACCOUNT_NO               VARCHAR(20) not null,
    PRF_AC_PRFT_SYSTEM       SMALLINT    not null,
    PROFITS_ACCOUNT_NUM      VARCHAR(40) not null,
    ROW_EFFECTIVE_DATE       DATE        not null,
    ROW_EXPIRATION_DATE      DATE,
    ROW_CURRENT_FLAG         DECIMAL(15),
    ENTRY_STATUS_IND_NAME    VARCHAR(8),
    PROFITS_ACCOUNT_CD       SMALLINT,
    CARD_CUSTOMER_ID         INTEGER,
    CARD_CUSTOMER_NAME       VARCHAR(107),
    CARD_START_DATE          DATE,
    CARD_END_DATE            DATE,
    ACCT_KEY                 DECIMAL(11),
    CARD_CUSTOMER_CD         SMALLINT,
    ACCT_PRODUCT_DESCRIPTION VARCHAR(40),
    ACCT_PRODUCT_ID          INTEGER,
    MONITORING_UNIT          INTEGER,
    constraint PK_W_FACT_CUSTOMER_CARD
        primary key (CARD_NO, ENTRY_STATUS_IND, ACCOUNT_NO, PRF_AC_PRFT_SYSTEM, PROFITS_ACCOUNT_NUM, ROW_EFFECTIVE_DATE)
);

CREATE PROCEDURE W_FACT_CUSTOMER_CARD ( )
  SPECIFIC SQL160620112708187
  LANGUAGE SQL
  NOT DETERMINISTIC
  EXTERNAL ACTION
  MODIFIES SQL DATA
  CALLED ON NULL INPUT
  INHERIT SPECIAL REGISTERS
  OLD SAVEPOINT LEVEL
BEGIN
UPDATE w_fact_customer_card
SET    row_expiration_date =
          UTILPKG.add_days (
             (SELECT scheduled_date FROM bank_parameters)
            ,-1)
      ,row_current_flag = 0
WHERE      row_current_flag = 1
       AND (card_no
           ,entry_status_ind
           ,account_no
           ,prf_ac_prft_system
           ,profits_account_num) IN (SELECT t.card_no
                                           ,t.entry_status_ind
                                           ,t.account_no
                                           ,t.prf_ac_prft_system
                                           ,t.profits_account_num
                                     FROM   w_fact_customer_card t
                                            INNER JOIN
                                            (SELECT card_no
                                                   ,entry_status_ind
                                                   ,account_no
                                                   ,prf_ac_prft_system
                                                   ,profits_account_num
                                                   ,entry_status_ind_name
                                                   ,profits_account_cd
                                                   ,card_customer_id
                                                   ,card_customer_cd
                                                   ,card_customer_name
                                                   ,card_start_date
                                                   ,card_end_date
                                                   ,acct_key
                                                   ,acct_product_description
                                                   ,acct_product_id
                                                   ,monitoring_unit
                                             FROM   w_fact_customer_card
                                             WHERE  row_current_flag = 1
                                             MINUS
                                             (SELECT COALESCE (
                                                        cust_card_account.fk_cust_card_incar
                                                       ,cust_card_info.card_no
                                                       ,' ')
                                                        card_no
                                                    ,NVL (
                                                        cust_card_info.entry_status
                                                       ,' ')
                                                        entry_status_ind
                                                    ,NVL (
                                                        cust_card_account.account_no
                                                       ,' ')
                                                        account_no
                                                    ,NVL (
                                                        cust_card_account.prf_ac_prft_system
                                                       ,0)
                                                        prf_ac_prft_system
                                                    ,NVL (
                                                        cust_card_account.profits_account_nu
                                                       ,' ')
                                                        profits_account_num
                                                    ,DECODE (
                                                        cust_card_info.entry_status
                                                       ,1, 'Active'
                                                       ,'Inactive')
                                                        entry_status_ind_name
                                                    ,cust_card_account.profits_account_cd
                                                    ,cust.c_digit
                                                        card_customer_cd
                                                    ,cust_card_info.fk_customercust_id
                                                        card_customer_id
                                                    ,cust.name_standard
                                                        card_customer_name
                                                    ,cust_card_info.start_date
                                                        card_start_date
                                                    ,cust_card_info.end_date
                                                        card_end_date
                                                    ,profits_account.account_ser_num
                                                        acct_key
                                                    ,product.description
                                                        acct_product_description
                                                    ,profits_account.product_id
                                                    ,profits_account.monotoring_unit
                                                        monitoring_unit
                                              FROM   (cust_card_info
                                                      FULL OUTER JOIN
                                                      cust_card_account
                                                         ON (cust_card_account.fk_cust_card_incar =
                                                                cust_card_info.card_no))
                                                     LEFT JOIN
                                                     w_stg_customer cust
                                                        ON (    cust_card_info.fk_customercust_id =
                                                                   cust_id
                                                            AND cust_id != 0)
                                                     LEFT JOIN
                                                     profits_account
                                                        ON (    profits_account_nu =
                                                                   account_number
                                                            AND prf_ac_prft_system =
                                                                   prft_system
                                                            AND LENGTH (
                                                                   TRIM (
                                                                      account_number))
                                                                   IS NOT NULL)
                                                     LEFT JOIN product
                                                        ON (id_product =
                                                               product_id)
                                              WHERE  cust_card_info.entry_status =
                                                        '1')) s
                                               ON (    t.card_no = s.card_no
                                                   AND t.entry_status_ind =
                                                          s.entry_status_ind
                                                   AND t.account_no =
                                                          s.account_no
                                                   AND t.prf_ac_prft_system =
                                                          s.prf_ac_prft_system
                                                   AND t.profits_account_num =
                                                          s.profits_account_num));
INSERT INTO w_fact_customer_card (
               card_no
              ,entry_status_ind
              ,account_no
              ,prf_ac_prft_system
              ,profits_account_num
              ,row_effective_date
              ,row_expiration_date
              ,row_current_flag
              ,entry_status_ind_name
              ,profits_account_cd
              ,card_customer_id
              ,card_customer_cd
              ,card_customer_name
              ,card_start_date
              ,card_end_date
              ,acct_key
              ,acct_product_description
              ,acct_product_id
              ,monitoring_unit)
   SELECT card_no
         ,entry_status_ind
         ,account_no
         ,prf_ac_prft_system
         ,profits_account_num
         , (SELECT scheduled_date FROM bank_parameters) row_effective_date
         ,DATE '9999-12-31' row_expiration_date
         ,1 row_current_flag
         ,entry_status_ind_name
         ,profits_account_cd
         ,card_customer_id
         ,card_customer_cd
         ,card_customer_name
         ,card_start_date
         ,card_end_date
         ,acct_key
         ,acct_product_description
         ,acct_product_id
         ,monitoring_unit
   FROM   ( (SELECT COALESCE (
                       cust_card_account.fk_cust_card_incar
                      ,cust_card_info.card_no
                      ,' ')
                       card_no
                   ,cust_card_info.entry_status entry_status_ind
                   ,NVL (cust_card_account.account_no, ' ') account_no
                   ,NVL (cust_card_account.prf_ac_prft_system, 0)
                       prf_ac_prft_system
                   ,NVL (cust_card_account.profits_account_nu, ' ')
                       profits_account_num
                   ,DECODE (
                       cust_card_info.entry_status
                      ,1, 'Active'
                      ,'Inactive')
                       entry_status_ind_name
                   ,cust_card_account.profits_account_cd
                   ,cust_card_info.fk_customercust_id card_customer_id
                   ,cust.c_digit card_customer_cd
                   ,cust.name_standard card_customer_name
                   ,cust_card_info.start_date card_start_date
                   ,cust_card_info.end_date card_end_date
                   ,profits_account.account_ser_num acct_key
                   ,product.description acct_product_description
                   ,profits_account.product_id acct_product_id
                   ,profits_account.monotoring_unit monitoring_unit
             FROM   (cust_card_info
                     FULL OUTER JOIN cust_card_account
                        ON (cust_card_account.fk_cust_card_incar =
                               cust_card_info.card_no))
                    LEFT JOIN w_stg_customer cust
                       ON (    cust_card_info.fk_customercust_id = cust_id
                           AND cust_id != 0)
                    LEFT JOIN profits_account
                       ON (    profits_account_nu = account_number
                           AND prf_ac_prft_system = prft_system
                           AND LENGTH (TRIM (account_number)) IS NOT NULL)
                    LEFT JOIN product ON (id_product = product_id)
             WHERE  cust_card_info.entry_status = '1')
           MINUS
           SELECT card_no
                 ,entry_status_ind
                 ,account_no
                 ,prf_ac_prft_system
                 ,profits_account_num
                 ,entry_status_ind_name
                 ,profits_account_cd
                 ,card_customer_id
                 ,card_customer_cd
                 ,card_customer_name
                 ,card_start_date
                 ,card_end_date
                 ,acct_key
                 ,acct_product_description
                 ,acct_product_id
                 ,monitoring_unit
           FROM   w_fact_customer_card
           WHERE  row_current_flag = 1);
END;

