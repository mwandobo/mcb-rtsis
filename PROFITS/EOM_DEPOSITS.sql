create table EOM_DEPOSITS
(
    EOM_DATE                      DATE                     not null,
    ACCOUNT_NUMBER                CHAR(40)                 not null,
    ACCOUNT_CD                    SMALLINT,
    RANKING                       DECIMAL(15, 2),
    DEP_OPEN_UNIT                 INTEGER,
    CUST_ID                       INTEGER,
    C_DIGIT                       SMALLINT,
    PRFT_SYSTEM                   SMALLINT,
    AGREEMENT_CD                  SMALLINT,
    AGREEMENT_SYSTEM              SMALLINT,
    ID_PRODUCT                    INTEGER,
    ID_CURRENCY                   INTEGER,
    DAYS_DURATION                 SMALLINT,
    REMAINING_DAYS                DECIMAL(15, 2),
    SERIAL_NUM_F                  INTEGER,
    SERIAL_NUM_P                  INTEGER,
    SERIAL_NUM_L                  INTEGER,
    RATE                          DECIMAL(8, 4),
    RENEWAL_SER_NUM               INTEGER,
    FIXING_RATE                   DECIMAL(12, 6),
    FKGD_CATEGORY                 INTEGER,
    FK2GENERIC_DETASER            INTEGER,
    FK_GENERIC_DETASER            INTEGER,
    FK_COLLABORATIOBAN            INTEGER,
    NOTIFICATION_DAYS             SMALLINT,
    LNS_UNITCODE                  INTEGER,
    LNS_ACC_TYPE                  SMALLINT,
    LNS_ACC_SN                    INTEGER,
    NORMAL_BAL                    DECIMAL(15, 2),
    OVERDUE_BAL                   DECIMAL(15, 2),
    INTEREST                      DECIMAL(15, 2),
    EURO_INTEREST                 DECIMAL(15, 2),
    AVER_INT_BAL                  DECIMAL(15, 2),
    EURO_AVER_INT_BAL             DECIMAL(15, 2),
    BOOK_BALANCE                  DECIMAL(15, 2),
    ACCOUNT_LIMIT                 DECIMAL(15, 2),
    EURO_BOOK_BAL                 DECIMAL(15, 2),
    START_DATE                    DATE,
    EXPIRY_DATE                   DATE,
    CUST_TYPE                     CHAR(1),
    NON_RESIDENT                  CHAR(1),
    EUR_IN_COUNTRY                CHAR(1),
    ENTRY_STATUS                  CHAR(1),
    ONE_ACCOUNT_FLG               CHAR(1),
    DEPOSIT_TYPE                  CHAR(1),
    FKGH_CATEGORY                 CHAR(5),
    FK2GENERIC_DETAFK             CHAR(5),
    FK_GENERIC_DETAFK             CHAR(5),
    PARAMETER_TYPE_L              CHAR(5),
    PARAMETER_TYPE_P              CHAR(5),
    PARAMETER_TYPE_F              CHAR(5),
    CR_CNTR_GL_ACC                CHAR(21),
    CR_INT_ACCR_GL_ACC            CHAR(21),
    CR_INT_GL_ACC                 CHAR(21),
    DR_CNTR_GL_ACC                CHAR(21),
    DR_INT_ACCR_GL_ACC            CHAR(21),
    DR_INT_GL_ACC                 CHAR(21),
    TD_CR_INT_ACCR_GL_ACC         CHAR(21),
    TD_CR_INT_GL_ACC              CHAR(21),
    TD_CR_INT_NOTAX_GL_AC         CHAR(21),
    TD_GL_ACCOUNT                 CHAR(21),
    TD_GL_ACCRUAL_ACC             CHAR(21),
    NRM_DR_CNTR_GL_ACC            CHAR(21),
    OV_DR_CNTR_GL_ACC             CHAR(21),
    FIRST_NAME                    CHAR(20),
    AGREEMENT_NUMBER              CHAR(40),
    SURNAME                       CHAR(70),
    UNIT_NAME                     VARCHAR(40),
    PRODUCT_DESC                  VARCHAR(40),
    FK0UNITCODE                   INTEGER,
    FK1GENERIC_DETAFK             CHAR(5),
    FK1GENERIC_DETASER            INTEGER,
    DEP_ACC_NUMBER                DECIMAL(11),
    CHECKING_ACC_TYPE             SMALLINT,
    ACCR_CR_INTEREST              DECIMAL(15, 2),
    ACCR_DB_INTEREST              DECIMAL(15, 2),
    FK1INTERESTID_INTE            INTEGER,
    FK0INTERESTID_INTE            INTEGER,
    FK_INTERESTID_INTE            INTEGER,
    UNCLEAR_BALANCE               DECIMAL(15, 2),
    IBAN                          VARCHAR(40),
    LAST_TRX_DATE                 DATE,
    OPENING_DATE                  DATE,
    AFM_NO                        CHAR(20),
    ID_NO                         VARCHAR(20),
    DEL_DESCRIPTION_ID            VARCHAR(40),
    UNUTILIZED_DAYS               DECIMAL(15, 2),
    AVAILABLE_BALANCE             DECIMAL(15, 2),
    BLOCKED_BALANCE               DECIMAL(15, 2),
    LAST_DORMANT_DATE             DATE,
    CLOSING_TOTAL                 DECIMAL(15, 2),
    EMP_FIRST_NAME                VARCHAR(40),
    EMP_LAST_NAME                 VARCHAR(70),
    LAST_TRX_AMOUNT               DECIMAL(15, 2),
    FK_BANKEMPLOYEEID             CHAR(8),
    PERIOD_INTEREST               DECIMAL(15, 2),
    PERIOD_INT_TAX                DECIMAL(15, 2),
    DEL_TAX_WITHHELD_TO_DATE_LCY  DECIMAL(15, 2),
    FINAL_WHLD_TAX_FLG            CHAR(1),
    PRODUCT_TAX_PERC              DECIMAL(8, 4),
    APPROV_CR_RATE                DECIMAL(8, 4),
    APPROV_DB_RATE                DECIMAL(8, 4),
    DURATION_UNIT                 CHAR(1),
    DURATION_VALUE                SMALLINT,
    TEMP_EXC_END_DT               DATE,
    TEMP_EXC_START_DT             DATE,
    CR_INT_PERC                   DECIMAL(9, 6),
    TRANSACTION_CODE              INTEGER,
    ACCOUNT_TYPE                  CHAR(1),
    PROD_IDLE_DURATION            SMALLINT,
    ACCR_EXCESS_INTER             DECIMAL(15, 2),
    ACCR_EXCESS_INT_LC            DECIMAL(15, 2),
    CR_PROGRESS_INTER             DECIMAL(15, 2),
    CR_PROGRESS_INT_LC            DECIMAL(15, 2),
    DB_PROGRESS_INTER             DECIMAL(15, 2),
    DB_PROGRESS_INT_LC            DECIMAL(15, 2),
    ACCR_EXC_PROGRESS             DECIMAL(15, 2),
    ACCR_EXC_PROGRE_LC            DECIMAL(15, 2),
    ACCR_CR_INTER_LC              DECIMAL(15, 2),
    ACCR_DB_INTER_LC              DECIMAL(15, 2),
    OVERDRAFT_COMMENT             CHAR(40),
    TEMPORARY_EXCESS              DECIMAL(15, 2),
    LAST_DATE_SWITCHED_2_NEGATIVE DATE,
    DEL_LAST_TUN_DATE             DATE,
    DEL_LAST_TUN_USR              CHAR(8),
    DEL_LAST_TUN_USRSN            INTEGER,
    DEL_LAST_TUN_UNIT             INTEGER,
    DEL_LAST_TUN_CODE             INTEGER,
    OVERDU_DB_ACCRUALS            DECIMAL(15, 2),
    NORMAL_DB_ACCRUALS            DECIMAL(15, 2),
    GROSS_TOTAL                   DECIMAL(15, 2),
    GROSS_TOTAL_LC                DECIMAL(15, 2),
    EXCESS_INTEREST_RATE          DECIMAL(9, 6),
    CURRENCY                      CHAR(5),
    ACCT_KEY                      DECIMAL(11)              not null,
    LOAN_ACCT_KEY                 DECIMAL(11)    default 0 not null,
    DATE_IN_EXCESS                DATE,
    CLOSING_DATE                  DATE,
    LAST_STATEMENT_NUM            INTEGER,
    CLOSING_AMT                   DECIMAL(15, 2),
    BENEFICIARIES_NAMES_STANDARD  VARCHAR(2000),
    ATM_CARD_FLAG                 VARCHAR(7),
    DEPOSIT_TYPE_IND              VARCHAR(14),
    CUSTOMER_NAME                 VARCHAR(300),
    CLOSING_FEE                   DECIMAL(15, 2),
    CLOSING_USER                  CHAR(8),
    CLOSING_UNIT                  INTEGER,
    ACCOUNT_NO                    VARCHAR(50),
    DEL_INTEREST_TO_DATE_LCY      DECIMAL(15, 2) default 0,
    STATUS_IND                    VARCHAR(15),
    DORMANT_CR_CNTR_GL_ACC        CHAR(21),
    INTEREST_TO_WITHDRAW          DECIMAL(15, 2) default 0,
    MONITORING_EMPLOYEE_NAME      VARCHAR(41),
    CLOAN_CATEGORY_DESCRIPTION    VARCHAR(40),
    DEBIT_INTEREST_RATE           DECIMAL(15, 2),
    SALESPERSON                   VARCHAR(40),
    FKGH_TYPE                     CHAR(5),
    FKGD_TYPE                     INTEGER,
    FKGH_RESIDES_IN_RE            CHAR(5),
    FKGD_RESIDES_IN_RE            INTEGER,
    CLOSED_AUTHORIZER             CHAR(8),
    I_COMMENTS                    VARCHAR(40),
    constraint IXU_EOM_003
        primary key (EOM_DATE, ACCOUNT_NUMBER)
);

create unique index IDX01_EOM_DEPOSITS
    on EOM_DEPOSITS (EOM_DATE, LOAN_ACCT_KEY);

create unique index IDX_EOM_DEPOSITS_CUST_ID
    on EOM_DEPOSITS (EOM_DATE, CUST_ID);

create unique index SO2_EOM_DEPOSITS
    on EOM_DEPOSITS (DEP_ACC_NUMBER);

create unique index UIX_EOMDEPOSITSACCT_KEY
    on EOM_DEPOSITS (EOM_DATE, ACCT_KEY);

CREATE PROCEDURE EOM_DEPOSITS ( )
  SPECIFIC SQL160620112634664
  LANGUAGE SQL
  NOT DETERMINISTIC
  EXTERNAL ACTION
  MODIFIES SQL DATA
  CALLED ON NULL INPUT
  INHERIT SPECIAL REGISTERS
  OLD SAVEPOINT LEVEL
BEGIN
DELETE eom_deposits
WHERE  eom_date = (SELECT scheduled_date FROM bank_parameters);
INSERT INTO eom_deposits (
               ranking
              ,dep_open_unit
              ,unit_name
              ,account_number
              ,account_cd
              ,entry_status
              ,expiry_date
              ,cust_id
              ,c_digit
              ,prft_system
              ,agreement_number
              ,agreement_cd
              ,agreement_system
              ,surname
              ,first_name
              ,id_product
              ,product_desc
              ,id_currency
              ,cust_type
              ,non_resident
              ,eur_in_country
              ,book_balance
              ,account_limit
              ,days_duration
              ,remaining_days
              ,parameter_type_f
              ,serial_num_f
              ,parameter_type_p
              ,serial_num_p
              ,parameter_type_l
              ,serial_num_l
              ,euro_book_bal
              ,deposit_type
              ,rate
              ,interest
              ,euro_interest
              ,aver_int_bal
              ,euro_aver_int_bal
              ,renewal_ser_num
              ,fixing_rate
              ,fkgh_category
              ,fkgd_category
              ,fk2generic_detafk
              ,fk2generic_detaser
              ,fk_generic_detafk
              ,fk_generic_detaser
              ,cr_cntr_gl_acc
              ,cr_int_accr_gl_acc
              ,cr_int_gl_acc
              ,dr_cntr_gl_acc
              ,dr_int_accr_gl_acc
              ,dr_int_gl_acc
              ,td_cr_int_accr_gl_acc
              ,td_cr_int_gl_acc
              ,td_cr_int_notax_gl_ac
              ,td_gl_account
              ,td_gl_accrual_acc
              ,fk_collaboratioban
              ,notification_days
              ,lns_unitcode
              ,lns_acc_type
              ,lns_acc_sn
              ,one_account_flg
              ,normal_bal
              ,overdue_bal
              ,nrm_dr_cntr_gl_acc
              ,ov_dr_cntr_gl_acc
              ,start_date
              ,eom_date
              ,fk0unitcode
              ,fk1generic_detafk
              ,fk1generic_detaser
              ,dep_acc_number
              ,checking_acc_type
              ,accr_cr_interest
              ,accr_db_interest
              ,iban
              ,opening_date
              ,last_trx_date
              ,fk_bankemployeeid
              ,period_interest
              ,period_int_tax
              ,account_type
              ,final_whld_tax_flg
              ,approv_cr_rate
              ,last_dormant_date
              ,overdraft_comment
              ,accr_cr_inter_lc
              ,emp_first_name
              ,emp_last_name
              ,unclear_balance
              ,prod_idle_duration
              ,overdu_db_accruals
              ,normal_db_accruals
              ,gross_total
              ,gross_total_lc
              ,acct_key
              ,currency
              ,loan_acct_key
              ,date_in_excess
              ,closing_date
              ,available_balance
              ,status_ind
              ,last_statement_num
              ,atm_card_flag
              ,deposit_type_ind
              ,customer_name
              ,account_no
              ,dormant_cr_cntr_gl_acc
              ,interest_to_withdraw
              ,monitoring_employee_name
              ,cloan_category_description
              ,salesperson
              ,excess_interest_rate
              ,debit_interest_rate
              ,FKGH_TYPE
             ,FKGD_TYPE
              ,FKGH_RESIDES_IN_RE,
              FKGD_RESIDES_IN_RE
              )
   WITH ain
        AS (SELECT   s.account_number dep_acc_number
                    ,MAX (TRIM (g1.description)) salesperson
            FROM     dep_acc_add_info s
                     LEFT JOIN generic_detail g1
                        ON (    TRIM (g1.short_description) =
                                   TRIM (s.text_data)
                            AND g1.fk_generic_headpar = 'ALCHS'
                            AND s.row_id = 2)
            GROUP BY s.account_number)
   SELECT r_deposit_account.renewal_number ranking
         ,profits_account.dep_open_unit
         ,unit.unit_name
         ,profits_account.account_number
         ,profits_account.account_cd
         ,d.entry_status
         ,tdr.expiry_date
         ,cust.cust_id
         ,cust.c_digit
         ,profits_account.prft_system
         ,'0' AS agreement_number
         ,0 AS agreement_cd
         ,0 AS agreement_system
         ,cust.surname
         ,cust.first_name
         ,profits_account.product_id id_product
         ,product.description AS product_desc
         ,r_deposit_account.fk_currencyid_curr id_currency
         ,stat_account_bal.cust_type
         ,stat_account_bal.non_resident
         ,stat_account_bal.eur_in_country
         ,stat_account_bal.book_balance
         ,r_deposit_account.account_limit
         ,stat_account_bal.days_duration
         ,CASE
             WHEN tdr.expiry_date <= DATE '1000-01-01'
             THEN
                0
             ELSE
                  tdr.expiry_date
                - (SELECT scheduled_date FROM bank_parameters)
          END
             remaining_days
         ,stat_account_bal.parameter_type_f
         ,stat_account_bal.serial_num_f
         ,stat_account_bal.parameter_type_p
         ,stat_account_bal.serial_num_p
         ,stat_account_bal.parameter_type_l
         ,stat_account_bal.serial_num_l
         ,stat_account_bal.euro_book_bal
         ,r_deposit_account.deposit_type
         ,stat_account_bal.rate
         ,stat_account_bal.interest
         ,stat_account_bal.euro_interest
         ,stat_account_bal.aver_int_bal
         ,stat_account_bal.euro_aver_int_bal
         ,stat_account_bal.renewal_ser_num
         ,stat_account_bal.fixing_rate
         ,r_deposit_account.fkgh_category
         ,r_deposit_account.fkgd_category
         ,r_deposit_account.fk2generic_detafk
         ,r_deposit_account.fk2generic_detaser
         ,r_deposit_account.fk_generic_detafk
         ,r_deposit_account.fk_generic_detaser
         ,class_gl.cr_cntr_gl_acc
         ,class_gl.cr_int_accr_gl_acc
         ,class_gl.cr_int_gl_acc
         ,class_gl.dr_cntr_gl_acc
         ,class_gl.dr_int_accr_gl_acc
         ,class_gl.dr_int_gl_acc
         ,'00.00.0000.0000      ' AS td_cr_int_accr_gl_acc
         ,'00.00.0000.0000      ' AS td_cr_int_gl_acc
         ,'00.00.0000.0000      ' AS td_cr_int_notax_gl_ac
         ,'00.00.0000.0000      ' AS td_gl_account
         ,'00.00.0000.0000      ' AS td_gl_accrual_acc
         ,r_deposit_account.fk_collaboratioban
         ,notification_days
         ,lns_unitcode
         ,lns_acc_type
         ,lns_acc_sn
         ,DECODE (NVL (lns_acc_sn, 0), 0, '0', '1') AS one_account_flg
         ,0 AS normal_bal
         ,0 AS overdue_bal
         ,' ' AS nrm_dr_cntr_gl_acc
         ,' ' AS ov_dr_cntr_gl_acc
         ,tdr.start_date
         , (SELECT scheduled_date FROM bank_parameters) AS eom_date
         ,r_deposit_account.fk0unitcode
         ,r_deposit_account.fk1generic_detafk
         ,r_deposit_account.fk1generic_detaser
         ,profits_account.dep_acc_number
         ,CASE
             WHEN     deposit.fk_generic_detafk = 'LACTP'
                  AND deposit.fk_generic_detaser IN (50, 31)
             THEN
                '1'
             ELSE
                '0'
          END
             checking_acc_type
         ,r_deposit_account.accr_cr_interest
         ,r_deposit_account.accr_db_interest
         ,stat_account_bal.iban
         ,r_deposit_account.opening_date
         ,r_deposit_account.last_trx_date
         ,r_deposit_account.fk_bankemployeeid
         ,period_interest
         ,period_int_tax
         ,r_deposit_account.account_type
         ,d.final_whld_tax_flg
         ,tdr.approv_cr_rate approv_cr_rate
         ,r_deposit_account.last_dormant_date
         ,' ' overdraft_comment
         ,r_deposit_account.accr_cr_interest * fixing_rate accr_cr_inter_lc
         ,bankemployee.first_name emp_first_name
         ,bankemployee.last_name emp_last_name
         ,r_deposit_account.unclear_balance
         ,idle_duration AS prod_idle_duration
         ,dep_account_info.overdu_db_accruals
         ,dep_account_info.normal_db_accruals
         ,  stat_account_bal.book_balance
          + DECODE (
               NVL (dep_account_info.lns_acc_sn, 0)
              ,0, 0
              ,  dep_account_info.overdu_db_accruals
               + dep_account_info.normal_db_accruals)
          + r_deposit_account.accr_db_interest
          + r_deposit_account.accr_cr_interest
             gross_total
         ,  stat_account_bal.euro_book_bal
          + DECODE (
               NVL (dep_account_info.lns_acc_sn, 0)
              ,0, 0
              ,    dep_account_info.overdu_db_accruals
                 * stat_account_bal.fixing_rate
               +   dep_account_info.normal_db_accruals
                 * stat_account_bal.fixing_rate)
          + (r_deposit_account.accr_db_interest * fixing_rate)
          + (r_deposit_account.accr_cr_interest * fixing_rate)
             gross_total_lc
         ,profits_account.account_ser_num acct_key
         ,cur.short_descr currency
         ,NVL (loanprfaccount.account_ser_num, 0) loan_acct_key
        ,r_deposit_account.date_in_excess
         ,r_deposit_account.closing_date
         ,r_deposit_account.available_balance
         ,DECODE (
             d.entry_status
            ,'0', 'Deleted'
            ,'1', 'Active'
            ,'2', 'Locked'
            ,'3', 'Closed by Cust.'
            ,'4', 'Closed by Bank'
            ,'5', 'Blocked'
            ,'6', 'Dormant'
            ,'7', 'Unfunded'
            ,'8', 'Inactive'
            ,'n/a')
             status_ind
         ,r_deposit_account.lst_statement_num last_statement_num
         ,DECODE (r_deposit_account.atm_card_flag, '1', 'ATM', 'Non-ATM')
             atm_card_flag
         ,DECODE (
             r_deposit_account.deposit_type
            ,'1', 'First Demand'
            ,'2', 'Time Deposit'
            ,'3', 'Certificate'
            ,'4', 'Instant Income'
            ,'5', 'Overdraft'
            ,'6', 'Commitment'
            ,'7', 'Regular Income'
            ,'n/a')
             deposit_type_ind
         ,cust.name_standard
         ,   TRIM (profits_account.account_number)
          || '-'
          || profits_account.account_cd
             account_no
         ,dormant_class_gl.cr_cntr_gl_acc dormant_cr_cntr_gl_acc
         ,r_deposit_account.interest_to_withdr
         ,TRIM (bankemployee.first_name) || ' ' || bankemployee.last_name
             monitoring_employee_name
         ,gd.description cloan_category_description
         ,ain.salesperson salesperson
         ,sd.excess_interest_rate excess_interest_rate
         ,sd.debit_interest_rate debit_interest_rate
        ,r_deposit_account.FKGH_TYPE
       ,r_deposit_account.FKGD_TYPE
        ,FKGH_RESIDES_IN_RE,
        FKGD_RESIDES_IN_RE
   FROM   profits_account
          INNER JOIN deposit_account d
             ON (d.account_number = profits_account.dep_acc_number)
          INNER JOIN r_deposit_account r_deposit_account
             ON (profits_account.dep_acc_number =
                    r_deposit_account.account_number)
          LEFT JOIN stat_account_bal
             ON (stat_account_bal.account_number =
                    r_deposit_account.account_number)
          LEFT JOIN product
             ON (profits_account.product_id = product.id_product)
          LEFT JOIN unit ON (unit.code = profits_account.dep_open_unit)
          LEFT JOIN deposit
             ON (r_deposit_account.fk_depositfk_produ =
                    deposit.fk_productid_produ)
          LEFT JOIN w_stg_customer cust
             ON (profits_account.cust_id = cust.cust_id)
          LEFT JOIN dep_account_info
             ON (dep_account_info.fk1deposit_accoacc =
                    r_deposit_account.account_number)
          LEFT JOIN bankemployee
             ON (r_deposit_account.fk_bankemployeeid = bankemployee.id)
          LEFT JOIN currency cur
             ON (cur.id_currency = r_deposit_account.fk_currencyid_curr)
          LEFT JOIN class_gl
             ON (    class_gl.fk_productid_produ =
                        r_deposit_account.fk_depositfk_produ
                 AND r_deposit_account.fk_generic_detafk =
                        class_gl.fk_generic_detafk
                 AND r_deposit_account.fk_generic_detaser =
                        class_gl.fk_generic_detaser
                 AND r_deposit_account.fkgh_category =
                        class_gl.fk_cust_categ_gh
                 AND r_deposit_account.fkgd_category =
                        class_gl.fk_cust_categ_gd
                 AND class_gl.loan_status = '1'
                 AND class_gl.entry_status = '1')
          LEFT JOIN class_gl dormant_class_gl
             ON (    dormant_class_gl.fk_productid_produ =
                        r_deposit_account.fk_depositfk_produ
                 AND r_deposit_account.fk_generic_detafk =
                        dormant_class_gl.fk_generic_detafk
                 AND r_deposit_account.fk_generic_detaser =
                        dormant_class_gl.fk_generic_detaser
                 AND r_deposit_account.fkgh_category =
                        dormant_class_gl.fk_cust_categ_gh
                 AND r_deposit_account.fkgd_category =
                        dormant_class_gl.fk_cust_categ_gd
                 AND dormant_class_gl.loan_status = '2'
                 AND dormant_class_gl.entry_status = '1')
          LEFT JOIN profits_account loanprfaccount
             ON (    loanprfaccount.lns_open_unit =
                        dep_account_info.lns_unitcode
                 AND loanprfaccount.lns_sn = dep_account_info.lns_acc_sn
                 AND loanprfaccount.lns_type = dep_account_info.lns_acc_type
                 AND loanprfaccount.prft_system = 4
                 AND dep_account_info.lns_unitcode > 0
                 AND stat_account_bal.book_balance < 0)
          LEFT JOIN r_time_depos_renew tdr
             ON (    r_deposit_account.account_number =
                        tdr.fk_deposit_accoacc
                 AND r_deposit_account.renewal_number = tdr.renewal_ser_num
                 AND tdr.entry_status != 0)
          LEFT JOIN generic_detail gd
             ON     gd.fk_generic_headpar = r_deposit_account.fkgh_category
                AND gd.serial_num = r_deposit_account.fkgd_category
          LEFT JOIN ain
             ON (ain.dep_acc_number = r_deposit_account.account_number)
          LEFT JOIN w_stg_deposit_account sd
             ON sd.account_number = profits_account.account_number
   WHERE      profits_account.prft_system = '3'
          AND r_deposit_account.deposit_type IN ('2', '3', '4')
   UNION ALL
   SELECT 0 AS ranking
         ,profits_account.dep_open_unit
         ,unit.unit_name
         ,profits_account.account_number
         ,profits_account.account_cd
         ,d.entry_status
         ,r_deposit_account.expiry_date
         ,cust.cust_id
         ,cust.c_digit
         ,profits_account.prft_system
         ,profits_account_1.account_number AS agreement_number
         ,profits_account_1.account_cd AS agreement_cd
         ,profits_account_1.prft_system AS agreement_system
         ,cust.surname
         ,cust.first_name
         ,profits_account.product_id id_product
         ,product.description AS product_desc
         ,r_deposit_account.fk_currencyid_curr id_currency
         ,stat_account_bal.cust_type
         ,stat_account_bal.non_resident
         ,stat_account_bal.eur_in_country
         ,stat_account_bal.book_balance
         ,r_deposit_account.account_limit
         ,stat_account_bal.days_duration
         ,CASE
             WHEN r_deposit_account.expiry_date <= DATE '1000-01-01'
             THEN
                0
             ELSE
                  r_deposit_account.expiry_date
                - (SELECT scheduled_date FROM bank_parameters)
          END
             remaining_days
         ,stat_account_bal.parameter_type_f
         ,stat_account_bal.serial_num_f
         ,stat_account_bal.parameter_type_p
         ,stat_account_bal.serial_num_p
         ,stat_account_bal.parameter_type_l
         ,stat_account_bal.serial_num_l
         ,stat_account_bal.euro_book_bal
         ,r_deposit_account.deposit_type
         ,stat_account_bal.rate
         ,stat_account_bal.interest
         ,stat_account_bal.euro_interest
         ,stat_account_bal.aver_int_bal
         ,stat_account_bal.euro_aver_int_bal
         ,stat_account_bal.renewal_ser_num
         ,stat_account_bal.fixing_rate
         ,r_deposit_account.fkgh_category
         ,r_deposit_account.fkgd_category
         ,r_deposit_account.fk2generic_detafk
         ,r_deposit_account.fk2generic_detaser
         ,r_deposit_account.fk_generic_detafk
         ,r_deposit_account.fk_generic_detaser
         ,class_gl.cr_cntr_gl_acc
         ,class_gl.cr_int_accr_gl_acc
         ,class_gl.cr_int_gl_acc
         ,class_gl.dr_cntr_gl_acc
         ,class_gl.dr_int_accr_gl_acc
         ,class_gl.dr_int_gl_acc
         ,'00.00.0000.0000      ' AS td_cr_int_accr_gl_acc
         ,'00.00.0000.0000      ' AS td_cr_int_gl_acc
         ,'00.00.0000.0000      ' AS td_cr_int_notax_gl_ac
         ,'00.00.0000.0000      ' AS td_gl_account
         ,'00.00.0000.0000      ' AS td_gl_accrual_acc
         ,r_deposit_account.fk_collaboratioban
         ,notification_days
         ,lns_unitcode
         ,lns_acc_type
         ,lns_acc_sn
         ,DECODE (NVL (lns_acc_sn, 0), 0, '0', '1') AS one_account_flg
         ,0 AS normal_bal
         ,0 AS overdue_bal
         ,' ' AS nrm_dr_cntr_gl_acc
         ,' ' AS ov_dr_cntr_gl_acc
         ,DATE '0001-01-01' AS start_date
         , (SELECT scheduled_date FROM bank_parameters) AS eom_date
         ,r_deposit_account.fk0unitcode
         ,r_deposit_account.fk1generic_detafk
         ,r_deposit_account.fk1generic_detaser
         ,profits_account.dep_acc_number
         ,CASE
             WHEN     deposit.fk_generic_detafk = 'LACTP'
                  AND deposit.fk_generic_detaser IN (50, 31)
             THEN
                '1'
             ELSE
                '0'
          END
             checking_acc_type
         ,r_deposit_account.accr_cr_interest
         ,r_deposit_account.accr_db_interest
         ,stat_account_bal.iban
         ,r_deposit_account.opening_date
         ,r_deposit_account.last_trx_date
         ,r_deposit_account.fk_bankemployeeid
         ,0 period_interest
         ,0 period_int_tax
         ,r_deposit_account.account_type
         ,d.final_whld_tax_flg
         ,NULL approv_cr_rate
         ,r_deposit_account.last_dormant_date
         ,r_deposit_account.overdraft_comment overdraft_comment
         ,r_deposit_account.accr_cr_interest * fixing_rate accr_cr_inter_lc
         ,bankemployee.first_name emp_first_name
         ,bankemployee.last_name emp_last_name
         ,r_deposit_account.unclear_balance
         ,idle_duration AS prod_idle_duration
         ,dep_account_info.overdu_db_accruals
         ,dep_account_info.normal_db_accruals
         ,  stat_account_bal.book_balance
          + DECODE (
               NVL (dep_account_info.lns_acc_sn, 0)
              ,0, 0
              ,  dep_account_info.overdu_db_accruals
               + dep_account_info.normal_db_accruals)
          + r_deposit_account.accr_db_interest
          + r_deposit_account.accr_cr_interest
             gross_total
         ,  stat_account_bal.euro_book_bal
          + DECODE (
               NVL (dep_account_info.lns_acc_sn, 0)
              ,0, 0
              ,    dep_account_info.overdu_db_accruals
                 * stat_account_bal.fixing_rate
               +   dep_account_info.normal_db_accruals
                 * stat_account_bal.fixing_rate)
          + (r_deposit_account.accr_db_interest * fixing_rate)
          + (r_deposit_account.accr_cr_interest * fixing_rate)
             gross_total_lc
         ,profits_account.account_ser_num acct_key
         ,cur.short_descr currency
         ,NVL (loanprfaccount.account_ser_num, 0) loan_acct_key
         ,r_deposit_account.date_in_excess
         ,r_deposit_account.closing_date
         ,r_deposit_account.available_balance
         ,DECODE (
             d.entry_status
            ,'0', 'Deleted'
            ,'1', 'Active'
            ,'2', 'Locked'
            ,'3', 'Closed by Cust.'
            ,'4', 'Closed by Bank'
            ,'5', 'Blocked'
            ,'6', 'Dormant'
            ,'7', 'Unfunded'
            ,'8', 'Inactive'
            ,'n/a')
             status_ind
         ,r_deposit_account.lst_statement_num last_statement_num
         ,DECODE (r_deposit_account.atm_card_flag, '1', 'ATM', 'Non-ATM')
             atm_card_flag
         ,DECODE (
             r_deposit_account.deposit_type
            ,'1', 'First Demand'
            ,'2', 'Time Deposit'
            ,'3', 'Certificate'
            ,'4', 'Instant Income'
            ,'5', 'Overdraft'
            ,'6', 'Commitment'
            ,'7', 'Regular Income'
            ,'n/a')
             deposit_type_ind
         ,cust.name_standard
         ,   TRIM (profits_account.account_number)
          || '-'
          || profits_account.account_cd
             account_no
         ,dormant_class_gl.cr_cntr_gl_acc
         ,r_deposit_account.interest_to_withdr
         ,TRIM (bankemployee.first_name) || ' ' || bankemployee.last_name
             monitoring_employee_name
         ,gd.description cloan_category_description
         ,ain.salesperson salesperson
         ,sd.excess_interest_rate excess_interest_rate
         ,sd.debit_interest_rate debit_interest_rate
         ,r_deposit_account.FKGH_TYPE
        ,r_deposit_account.FKGD_TYPE
       ,FKGH_RESIDES_IN_RE,
        FKGD_RESIDES_IN_RE
   FROM   profits_account
          INNER JOIN deposit_account d
             ON (d.account_number = profits_account.dep_acc_number)
          INNER JOIN r_deposit_account r_deposit_account
             ON (profits_account.dep_acc_number =
                    r_deposit_account.account_number)
          LEFT JOIN stat_account_bal
             ON (stat_account_bal.account_number =
                    r_deposit_account.account_number)
          LEFT JOIN product
             ON (profits_account.product_id = product.id_product)
          LEFT JOIN unit ON (unit.code = profits_account.dep_open_unit)
          LEFT JOIN deposit
             ON (r_deposit_account.fk_depositfk_produ =
                    deposit.fk_productid_produ)
          LEFT JOIN w_stg_customer cust
             ON (profits_account.cust_id = cust.cust_id)
          LEFT JOIN dep_account_info
             ON (dep_account_info.fk1deposit_accoacc =
                    r_deposit_account.account_number)
          LEFT JOIN bankemployee
             ON (r_deposit_account.fk_bankemployeeid = bankemployee.id)
          LEFT JOIN currency cur
             ON (cur.id_currency = r_deposit_account.fk_currencyid_curr)
          LEFT JOIN class_gl
             ON (    class_gl.fk_productid_produ =
                        r_deposit_account.fk_depositfk_produ
                 AND r_deposit_account.fk_generic_detafk =
                        class_gl.fk_generic_detafk
                 AND r_deposit_account.fk_generic_detaser =
                        class_gl.fk_generic_detaser
                 AND r_deposit_account.fkgh_category =
                        class_gl.fk_cust_categ_gh
                 AND r_deposit_account.fkgd_category =
                        class_gl.fk_cust_categ_gd
                 AND class_gl.loan_status = '1'
                 AND class_gl.entry_status = '1')
          LEFT JOIN class_gl dormant_class_gl
             ON (    dormant_class_gl.fk_productid_produ =
                        r_deposit_account.fk_depositfk_produ
                 AND r_deposit_account.fk_generic_detafk =
                        dormant_class_gl.fk_generic_detafk
                 AND r_deposit_account.fk_generic_detaser =
                        dormant_class_gl.fk_generic_detaser
                 AND r_deposit_account.fkgh_category =
                        dormant_class_gl.fk_cust_categ_gh
                 AND r_deposit_account.fkgd_category =
                        dormant_class_gl.fk_cust_categ_gd
                 AND dormant_class_gl.loan_status = '2'
                 AND dormant_class_gl.entry_status = '1')
          LEFT JOIN profits_account loanprfaccount
             ON (    loanprfaccount.lns_open_unit =
                        dep_account_info.lns_unitcode
                 AND loanprfaccount.lns_sn = dep_account_info.lns_acc_sn
                 AND loanprfaccount.lns_type = dep_account_info.lns_acc_type
                 AND loanprfaccount.prft_system = 4
                 AND dep_account_info.lns_unitcode > 0
                 AND stat_account_bal.book_balance < 0)
          LEFT JOIN profits_account profits_account_1
             ON (    profits_account_1.agr_unit =
                        r_deposit_account.fk_agreementfk_uni
                 AND profits_account_1.agr_membership_sn =
                        r_deposit_account.fk_agreementagr_me
                 AND profits_account_1.agr_sn =
                        r_deposit_account.fk_agreementagr_sn
                 AND profits_account_1.agr_year =
                        r_deposit_account.fk_agreementagr_ye
                 AND profits_account_1.prft_system = '19')
          LEFT JOIN generic_detail gd
             ON     gd.fk_generic_headpar = r_deposit_account.fkgh_category
                AND gd.serial_num = r_deposit_account.fkgd_category
          LEFT JOIN ain
             ON (ain.dep_acc_number = r_deposit_account.account_number)
          LEFT JOIN w_stg_deposit_account sd
             ON sd.account_number = profits_account.account_number
   WHERE      profits_account.prft_system = '3'
          AND r_deposit_account.deposit_type = '5'
   UNION ALL
   SELECT 0 AS ranking
         ,profits_account.dep_open_unit
         ,unit.unit_name
         ,profits_account.account_number
         ,profits_account.account_cd
         ,d.entry_status
         ,r_deposit_account.expiry_date
         ,cust.cust_id
         ,cust.c_digit
         ,profits_account.prft_system
         ,'0' AS agreement_number
         ,0 AS agreement_cd
         ,0 AS agreement_system
         ,cust.surname
         ,cust.first_name
         ,profits_account.product_id id_product
         ,product.description AS product_desc
         ,r_deposit_account.fk_currencyid_curr id_currency
         ,stat_account_bal.cust_type
         ,stat_account_bal.non_resident
         ,stat_account_bal.eur_in_country
         ,stat_account_bal.book_balance
         ,r_deposit_account.account_limit
         ,stat_account_bal.days_duration
         ,0 remaining_days
         ,stat_account_bal.parameter_type_f
         ,stat_account_bal.serial_num_f
         ,stat_account_bal.parameter_type_p
         ,stat_account_bal.serial_num_p
         ,stat_account_bal.parameter_type_l
         ,stat_account_bal.serial_num_l
         ,stat_account_bal.euro_book_bal
         ,r_deposit_account.deposit_type
         ,stat_account_bal.rate
         ,stat_account_bal.interest
         ,stat_account_bal.euro_interest
         ,stat_account_bal.aver_int_bal
         ,stat_account_bal.euro_aver_int_bal
         ,stat_account_bal.renewal_ser_num
         ,stat_account_bal.fixing_rate
         ,r_deposit_account.fkgh_category
         ,r_deposit_account.fkgd_category
         ,r_deposit_account.fk2generic_detafk
         ,r_deposit_account.fk2generic_detaser
         ,r_deposit_account.fk_generic_detafk
         ,r_deposit_account.fk_generic_detaser
         ,class_gl.cr_cntr_gl_acc
         ,class_gl.cr_int_accr_gl_acc
         ,class_gl.cr_int_gl_acc
         ,class_gl.dr_cntr_gl_acc
         ,class_gl.dr_int_accr_gl_acc
         ,class_gl.dr_int_gl_acc
         ,'00.00.0000.0000      ' AS td_cr_int_accr_gl_acc
         ,'00.00.0000.0000      ' AS td_cr_int_gl_acc
         ,'00.00.0000.0000      ' AS td_cr_int_notax_gl_ac
         ,'00.00.0000.0000      ' AS td_gl_account
         ,'00.00.0000.0000      ' AS td_gl_accrual_acc
         ,r_deposit_account.fk_collaboratioban
         ,notification_days
         ,lns_unitcode
         ,lns_acc_type
         ,lns_acc_sn
         ,DECODE (NVL (lns_acc_sn, 0), 0, '0', '1') AS one_account_flg
         ,0 AS normal_bal
         ,0 AS overdue_bal
         ,' ' AS nrm_dr_cntr_gl_acc
         ,' ' AS ov_dr_cntr_gl_acc
         ,DATE '0001-01-01' AS start_date
         , (SELECT scheduled_date FROM bank_parameters) AS eom_date
         ,r_deposit_account.fk0unitcode
         ,r_deposit_account.fk1generic_detafk
         ,r_deposit_account.fk1generic_detaser
         ,profits_account.dep_acc_number
         ,CASE
             WHEN     deposit.fk_generic_detafk = 'LACTP'
                  AND deposit.fk_generic_detaser IN (50, 31)
             THEN
                '1'
             ELSE
                '0'
          END
             checking_acc_type
         ,r_deposit_account.accr_cr_interest
         ,r_deposit_account.accr_db_interest
         ,stat_account_bal.iban
         ,r_deposit_account.opening_date
         ,r_deposit_account.last_trx_date
         ,r_deposit_account.fk_bankemployeeid
         ,0 period_interest
         ,0 period_int_tax
         ,r_deposit_account.account_type
         ,d.final_whld_tax_flg
         ,NULL approv_cr_rate
         ,r_deposit_account.last_dormant_date
         ,' ' overdraft_comment
         ,r_deposit_account.accr_cr_interest * fixing_rate accr_cr_inter_lc
         ,bankemployee.first_name emp_first_name
         ,bankemployee.last_name emp_last_name
         ,r_deposit_account.unclear_balance
         ,idle_duration AS prod_idle_duration
         ,dep_account_info.overdu_db_accruals
         ,dep_account_info.normal_db_accruals
         ,  stat_account_bal.book_balance
          + DECODE (
               NVL (dep_account_info.lns_acc_sn, 0)
              ,0, 0
              ,  dep_account_info.overdu_db_accruals
               + dep_account_info.normal_db_accruals)
          + r_deposit_account.accr_db_interest
          + r_deposit_account.accr_cr_interest
             gross_total
         ,  stat_account_bal.euro_book_bal
          + DECODE (
               NVL (dep_account_info.lns_acc_sn, 0)
              ,0, 0
              ,    dep_account_info.overdu_db_accruals
                 * stat_account_bal.fixing_rate
               +   dep_account_info.normal_db_accruals
                 * stat_account_bal.fixing_rate)
          + (r_deposit_account.accr_db_interest * fixing_rate)
          + (r_deposit_account.accr_cr_interest * fixing_rate)
             gross_total_lc
         ,profits_account.account_ser_num acct_key
         ,cur.short_descr currency
         ,NVL (loanprfaccount.account_ser_num, 0) loan_acct_key
         ,r_deposit_account.date_in_excess
         ,r_deposit_account.closing_date
         ,r_deposit_account.available_balance
         ,DECODE (
             d.entry_status
            ,'0', 'Deleted'
            ,'1', 'Active'
            ,'2', 'Locked'
            ,'3', 'Closed by Cust.'
            ,'4', 'Closed by Bank'
            ,'5', 'Blocked'
            ,'6', 'Dormant'
            ,'7', 'Unfunded'
            ,'8', 'Inactive'
            ,'n/a')
             status_ind
         ,r_deposit_account.lst_statement_num last_statement_num
         ,DECODE (r_deposit_account.atm_card_flag, '1', 'ATM', 'Non-ATM')
             atm_card_flag
         ,DECODE (
             r_deposit_account.deposit_type
            ,'1', 'First Demand'
            ,'2', 'Time Deposit'
            ,'3', 'Certificate'
            ,'4', 'Instant Income'
            ,'5', 'Overdraft'
            ,'6', 'Commitment'
            ,'7', 'Regular Income'
            ,'n/a')
             deposit_type_ind
         ,cust.name_standard
         ,   TRIM (profits_account.account_number)
          || '-'
          || profits_account.account_cd
             account_no
         ,dormant_class_gl.cr_cntr_gl_acc
         ,r_deposit_account.interest_to_withdr
         ,TRIM (bankemployee.first_name) || ' ' || bankemployee.last_name
             monitoring_employee_name
         ,gd.description cloan_category_description
         ,ain.salesperson salesperson
         ,sd.excess_interest_rate excess_interest_rate
         ,sd.debit_interest_rate debit_interest_rate
         ,r_deposit_account.FKGH_TYPE
        ,r_deposit_account.FKGD_TYPE
       ,FKGH_RESIDES_IN_RE,
        FKGD_RESIDES_IN_RE
   FROM   profits_account
          INNER JOIN deposit_account d
             ON (d.account_number = profits_account.dep_acc_number)
          INNER JOIN r_deposit_account r_deposit_account
             ON (profits_account.dep_acc_number =
                    r_deposit_account.account_number)
          LEFT JOIN stat_account_bal
             ON (stat_account_bal.account_number =
                    r_deposit_account.account_number)
          LEFT JOIN product
             ON (profits_account.product_id = product.id_product)
          LEFT JOIN unit ON (unit.code = profits_account.dep_open_unit)
          LEFT JOIN deposit
             ON (r_deposit_account.fk_depositfk_produ =
                    deposit.fk_productid_produ)
          LEFT JOIN w_stg_customer cust
            ON (profits_account.cust_id = cust.cust_id)
          LEFT JOIN dep_account_info
             ON (dep_account_info.fk1deposit_accoacc =
                    r_deposit_account.account_number)
          LEFT JOIN bankemployee
             ON (r_deposit_account.fk_bankemployeeid = bankemployee.id)
          LEFT JOIN currency cur
             ON (cur.id_currency = r_deposit_account.fk_currencyid_curr)
          LEFT JOIN class_gl
             ON (    class_gl.fk_productid_produ =
                        r_deposit_account.fk_depositfk_produ
                 AND r_deposit_account.fk_generic_detafk =
                        class_gl.fk_generic_detafk
                 AND r_deposit_account.fk_generic_detaser =
                        class_gl.fk_generic_detaser
                 AND r_deposit_account.fkgh_category =
                        class_gl.fk_cust_categ_gh
                 AND r_deposit_account.fkgd_category =
                        class_gl.fk_cust_categ_gd
                 AND class_gl.loan_status = '1'
                 AND class_gl.entry_status = '1')
          LEFT JOIN class_gl dormant_class_gl
             ON (    dormant_class_gl.fk_productid_produ =
                        r_deposit_account.fk_depositfk_produ
                 AND r_deposit_account.fk_generic_detafk =
                        dormant_class_gl.fk_generic_detafk
                 AND r_deposit_account.fk_generic_detaser =
                        dormant_class_gl.fk_generic_detaser
                 AND r_deposit_account.fkgh_category =
                        dormant_class_gl.fk_cust_categ_gh
                 AND r_deposit_account.fkgd_category =
                        dormant_class_gl.fk_cust_categ_gd
                 AND dormant_class_gl.loan_status = '2'
                AND dormant_class_gl.entry_status = '1')
          LEFT JOIN profits_account loanprfaccount
             ON (    loanprfaccount.lns_open_unit =
                        dep_account_info.lns_unitcode
                 AND loanprfaccount.lns_sn = dep_account_info.lns_acc_sn
                 AND loanprfaccount.lns_type = dep_account_info.lns_acc_type
                 AND loanprfaccount.prft_system = 4
                 AND dep_account_info.lns_unitcode > 0
                 AND stat_account_bal.book_balance < 0)
          LEFT JOIN generic_detail gd
             ON     gd.fk_generic_headpar = r_deposit_account.fkgh_category
                AND gd.serial_num = r_deposit_account.fkgd_category
          LEFT JOIN ain
             ON (ain.dep_acc_number = r_deposit_account.account_number)
          LEFT JOIN w_stg_deposit_account sd
             ON sd.account_number = profits_account.account_number
   WHERE      profits_account.prft_system = '3'
          AND r_deposit_account.deposit_type IN ('1', '6');
UPDATE eom_deposits
SET    beneficiaries_names_standard =
          (SELECT   LISTAGG (TRIM (w_stg_customer.name_standard), ', ')
                       WITHIN GROUP (ORDER BY beneficiary_sn)
           FROM     beneficiary
                    JOIN w_stg_customer ON fk_customercust_id = cust_id
                    JOIN profits_account
                       ON fk_deposit_accoacc = profits_account.dep_acc_number
           WHERE        eom_deposits.dep_acc_number =
                           profits_account.dep_acc_number
                    AND profits_account.dep_acc_number > 0
                    AND profits_account.prft_system = 3
           GROUP BY fk_deposit_accoacc)
WHERE  eom_date = (SELECT scheduled_date FROM bank_parameters);
UPDATE eom_deposits da
SET    (da.normal_bal, da.overdue_bal) =
          (SELECT   la.nrm_cap_bal
                  + la.nrm_rl_int_bal
                  + la.nrm_exp_bal
                  + la.nrm_com_bal
                     AS normal_bal
                 ,  la.ov_cap_bal
                  + la.ov_rl_nrm_int_bal
                  + la.ov_rl_pnl_int_bal
                  + la.ov_exp_bal
                  + la.ov_com_bal
                     AS overdue_bal
           FROM   r_loan_account la
           WHERE      da.lns_unitcode = la.fk_unitcode
                  AND da.lns_acc_type = la.acc_type
                  AND da.lns_acc_sn = la.acc_sn)
WHERE      da.lns_acc_sn <> 0
       AND da.book_balance < 0
       AND eom_date = (SELECT scheduled_date FROM bank_parameters);
UPDATE eom_deposits c
SET    (
          closing_unit
         ,closing_user
         ,closing_fee
         ,closing_amt
         ,last_trx_amount
         ,last_date_switched_2_negative) =
          (SELECT NVL (
                     MAX (
                        CASE
                           WHEN     c.status_ind IN ('Closed by Cust.'
                                                    ,'Closed by Bank')
                                AND (   (    w_fact_dep_acct_ledger.transaction_id =
                                                3241
                                         AND c.deposit_type <> '2')
                                     OR (    transaction_id IN (3451, 3441)
                                         AND c.deposit_type = '2'))
                           THEN
                              w_fact_dep_acct_ledger.trx_unit
                        END)
                    ,100)
                     closing_unit
                 ,NVL (
                     MAX (
                        CASE
                           WHEN     c.status_ind IN ('Closed by Cust.'
                                                    ,'Closed by Bank')
                                AND (   (    w_fact_dep_acct_ledger.transaction_id =
                                                3241
                                         AND c.deposit_type <> '2')
                                     OR (    transaction_id IN (3451, 3441)
                                         AND c.deposit_type = '2'))
                           THEN
                              w_fact_dep_acct_ledger.trx_user
                        END)
                    ,'999U5041')
                     closing_user
                 ,NVL (
                     SUM (
                        CASE
                           WHEN     c.status_ind IN ('Closed by Cust.'
                                                    ,'Closed by Bank')
                                AND (   (    w_fact_dep_acct_ledger.transaction_id =
                                                3241
                                         AND c.deposit_type <> '2'
                                         AND w_fact_dep_acct_ledger.justific_id =
                                                '93902')
                                     OR (    transaction_id IN (3451, 3441)
                                         AND w_fact_dep_acct_ledger.justific_id =
                                                '93925'
                                         AND c.deposit_type = '2'))
                           THEN
                              w_fact_dep_acct_ledger.entry_amount
                        END)
                    ,0)
                     closing_fee
                 ,NVL (
                     ABS (
                        SUM (
                           CASE
                              WHEN     justific_id < 93000
                                   AND c.status_ind IN ('Closed by Cust.'
                                                       ,'Closed by Bank')
                              THEN
                                 effective_amt
                           END))
                    ,0)
                     closing_amt
                 ,NVL (
                     SUM (
                        CASE trans_ser_num
                           WHEN last_statement_num THEN effective_amt
                        END)
                    ,0)
                     last_trx_amount
                 ,MAX (
                     CASE
                        WHEN     effective_amt + prev_acc_balance < 0
                             AND prev_acc_balance >= 0
                             AND c.book_balance < 0
                        THEN
                           trx_date
                     END)
                     last_date_switched_2_negative
           FROM   w_fact_dep_acct_ledger
           WHERE      (c.acct_key = w_fact_dep_acct_ledger.acct_key)
                  AND w_fact_dep_acct_ledger.printed_flag = 'Printed')
WHERE  c.eom_date = (SELECT scheduled_date FROM bank_parameters);
END;

