create table W_EOM_LG_ACCOUNT
(
    EOM_DATE                       DATE,
    ACCT_KEY                       DECIMAL(11),
    ACCOUNT_NUMBER                 CHAR(40),
    ACCOUNT_CD                     SMALLINT,
    CUST_ID                        INTEGER,
    CUST_CD                        SMALLINT,
    CUSTOMER_NAME                  VARCHAR(107),
    PRFT_SYSTEM                    SMALLINT,
    LG_ACCOUNT_TYPE                SMALLINT,
    LG_ACCOUNT_TYPE_DESCRIPTION    VARCHAR(40),
    LG_BODY_IND                    CHAR(1),
    LG_BODY_IND_NAME               VARCHAR(16),
    OVERDUE_FLAG                   CHAR(1),
    OVERDUE_FLAG_NAME              VARCHAR(11),
    EXTENSION_FLAG                 CHAR(1),
    EXTENSION_FLAG_NAME            VARCHAR(13),
    COMM_CHARGE_FREQUENCY          SMALLINT,
    COMM_CHARGE_FREQ_TYPE_IND      CHAR(1),
    COMM_CHARGE_FREQ_TYPE_IND_NAME VARCHAR(6),
    REPLACEMENT_REASON             CHAR(40),
    ACCOUNT_STATUS_IND             CHAR(1),
    ACCOUNT_STATUS_IND_NAME        VARCHAR(9),
    OBLIGATION_STATUS_IND          CHAR(1),
    OBLIGATION_STATUS_IND_NAME     VARCHAR(24),
    ACCOUNT_OPEN_DATE              DATE,
    LANGUAGE_CODE                  INTEGER,
    LANGUAGE_DESCRIPTION           VARCHAR(40),
    EXPIRATION_TYPE_IND            CHAR(1),
    EXPIRATION_TYPE_IND_NAME       VARCHAR(8),
    EXPIRY_DATE                    DATE,
    GUARANTEE_DATE                 DATE,
    RELEASE_DATE                   DATE,
    DEFAULT_TYPE                   CHAR(1),
    DEFAULT_DATE                   DATE,
    DEFAULT_AMOUNT                 DECIMAL(15, 2),
    REQUEST_DEFAULT_TYPE           CHAR(1),
    REQUEST_DEFAULT_TYPE_NAME      VARCHAR(25),
    REQUEST_DEFAULT_DATE           DATE,
    REQUEST_DEFAULT_AMOUNT         DECIMAL(15, 2),
    NEXT_COMMISSION_DATE           DATE,
    BALANCE_AMOUNT                 DECIMAL(15, 2),
    TOLERANCE_AMOUNT               DECIMAL(8, 4),
    INITIAL_AMOUNT                 DECIMAL(15, 2),
    LIMIT_AMOUNT                   DECIMAL(15, 2),
    DEPOSIT_ACCT_KEY               DECIMAL(11),
    DEPOSIT_ACCOUNT_NUMBER         CHAR(40),
    DEPOSIT_ACCOUNT_CD             SMALLINT,
    UNREALISED_COMMISS_FLAG        CHAR(1),
    UNREALISED_COMMISS_FLAG_NAME   VARCHAR(10),
    STOP_COMMISSION_DATE           DATE,
    STOP_COMMISSION_USER_CODE      CHAR(8),
    PRODUCT_ID                     INTEGER,
    PRODUCT_DESCRIPTION            VARCHAR(40),
    BENEFICIARY_CUST_ID            INTEGER,
    BENEFICIARY_CUST_CD            SMALLINT,
    BENEFICIARY_CUST_NAME          VARCHAR(107),
    AGREEMENT_ACCT_KEY             DECIMAL(11),
    AGREEMENT_ACCOUNT_NUMBER       CHAR(40),
    AGREEMENT_ACCOUNT_CD           SMALLINT,
    UNIT_CODE                      INTEGER,
    CHARGES_CURRENCY_ID            INTEGER,
    CHARGES_CURRENCY               CHAR(5),
    ISSUE_CUSTOMER_ID              INTEGER,
    MODIFICATION_USER_CODE         CHAR(8),
    MODIFICATION_USER_NAME         VARCHAR(41),
    MOVEMENT_CURRENCY_ID           INTEGER,
    MOVEMENT_CURRENCY              CHAR(5),
    OPEN_UNIT_CODE                 INTEGER,
    OPEN_UNIT_NAME                 VARCHAR(40),
    PRINT_UNIT_CODE                INTEGER,
    PRINT_UNIT_NAME                VARCHAR(40),
    LIMITS_CURRENCY_ID             INTEGER,
    LIMITS_CURRENCY                CHAR(5),
    ISSUANCE_JUSTIFICATION         VARCHAR(400),
    GROSS_TOTAL                    DECIMAL(15, 2),
    GROSS_TOTAL_LCY                DECIMAL(15, 2),
    BALANCE_AMOUNT_LCY             DECIMAL(15, 2),
    FIXING_RATE                    DECIMAL(12, 6),
    TOT_COMMISSION_BALANCE         DECIMAL(15, 2)
);

create unique index PK_W_EOM_LG_ACCOUNT
    on W_EOM_LG_ACCOUNT (EOM_DATE, ACCT_KEY);

CREATE PROCEDURE W_EOM_LG_ACCOUNT ( )
  SPECIFIC SQL160620112707784
  LANGUAGE SQL
  NOT DETERMINISTIC
 EXTERNAL ACTION
  MODIFIES SQL DATA
  CALLED ON NULL INPUT
  INHERIT SPECIAL REGISTERS
  OLD SAVEPOINT LEVEL
BEGIN
DELETE w_eom_lg_account
WHERE  eom_date = (SELECT scheduled_date FROM bank_parameters);
INSERT INTO w_eom_lg_account (
               eom_date
              ,acct_key
              ,account_number
              ,account_cd
              ,cust_id
              ,cust_cd
              ,customer_name
              ,prft_system
              ,lg_account_type
              ,lg_account_type_description
              ,lg_body_ind
              ,lg_body_ind_name
              ,overdue_flag
              ,overdue_flag_name
              ,extension_flag
              ,extension_flag_name
              ,comm_charge_frequency
              ,comm_charge_freq_type_ind
              ,comm_charge_freq_type_ind_name
              ,replacement_reason
              ,account_status_ind
              ,account_status_ind_name
              ,obligation_status_ind
              ,obligation_status_ind_name
              ,account_open_date
              ,language_code
              ,language_description
              ,expiration_type_ind
              ,expiration_type_ind_name
              ,expiry_date
              ,guarantee_date
              ,release_date
              ,default_type
              ,default_date
              ,default_amount
              ,request_default_type
              ,request_default_type_name
              ,request_default_date
              ,request_default_amount
              ,next_commission_date
              ,balance_amount
              ,tolerance_amount
              ,initial_amount
              ,limit_amount
              ,deposit_acct_key
              ,deposit_account_number
              ,deposit_account_cd
              ,unrealised_commiss_flag
              ,unrealised_commiss_flag_name
              ,stop_commission_date
              ,stop_commission_user_code
              ,product_id
              ,product_description
              ,beneficiary_cust_id
              ,beneficiary_cust_cd
              ,beneficiary_cust_name
              ,agreement_acct_key
              ,agreement_account_number
              ,agreement_account_cd
              ,unit_code
              ,charges_currency_id
              ,charges_currency
              ,issue_customer_id
              ,modification_user_code
              ,modification_user_name
              ,movement_currency_id
              ,movement_currency
              ,open_unit_code
              ,open_unit_name
              ,print_unit_code
              ,print_unit_name
              ,limits_currency_id
              ,limits_currency
              ,issuance_justification
              ,gross_total
              ,gross_total_lcy
              ,fixing_rate
              ,balance_amount_lcy)
   SELECT scheduled_date eom_date
         ,profits_account.account_ser_num acct_key
         ,profits_account.account_number
         ,profits_account.account_cd
         ,cust.cust_id
         ,cust.c_digit cust_cd
         ,cust.name_standard customer_name
         ,profits_account.prft_system
         ,r_lg_account.acc_type lg_account_type
         ,lgtyp.description lg_account_type_description
         ,r_lg_account.lg_body lg_body_ind
         ,DECODE (
             r_lg_account.lg_body
            ,'0', 'Not Returned'
            ,'1', 'Node'
            ,'2', 'Central Division'
            ,'3', 'Unit'
            ,'n/a')
             lg_body_ind_name
         ,r_lg_account.lg_overdue overdue_flag
         ,DECODE (
             r_lg_account.lg_overdue
            ,'1', 'Overdue'
            ,'2', 'Non-overdue'
            ,'n/a')
             overdue_flag_name
         ,r_lg_account.lg_extension extension_flag
         ,DECODE (
             r_lg_account.lg_extension
            ,'1', 'Extension'
            ,'2', 'Non-extension'
            ,'n/a')
             extension_flag_name
         ,r_lg_account.com_charge_frq comm_charge_frequency
         ,r_lg_account.com_charge_frq_typ comm_charge_freq_type_ind
         ,DECODE (
             r_lg_account.com_charge_frq_typ
            ,'1', 'Months'
            ,'2', 'Days'
            ,'n/a')
             comm_charge_freq_type_ind_name
         ,r_lg_account.replacement_reason
         ,r_lg_account.acc_status account_status_ind
         ,DECODE (
             r_lg_account.acc_status
            ,'0', 'For Issue'
            ,'1', 'Issued'
            ,'2', 'Matured'
            ,'3', 'Canceled'
            ,'4', 'Released'
            ,'5', 'Scheduled'
            ,'6', 'Closed'
            ,'n/a')
             account_status_ind_name
         ,r_lg_account.obligation_status obligation_status_ind
         ,DECODE (
             r_lg_account.obligation_status
            ,'0', 'Inactive'
            ,'1', 'Active'
            ,'4', 'Appl Partial With Delete'
            ,'2', 'Partial Forfeit'
            ,'3', 'Total Forfeit'
            ,'5', 'Appl. Partial no Delete'
            ,'6', 'Appl. Forfeit'
            ,'n/a')
             obligation_status_ind_name
         ,r_lg_account.acc_open_dt account_open_date
         ,r_lg_account.language_code language_code
         ,langu.description language_description
         ,r_lg_account.expiration_type expiration_type_ind
         ,DECODE (
             r_lg_account.expiration_type
            ,'1', 'Infinite'
            ,'2', 'Regular'
            ,'n/a')
             expiration_type_ind_name
         ,r_lg_account.expiry_dt expiry_date
         ,r_lg_account.guarantee_dt guarantee_date
         ,r_lg_account.release_dt release_date
         ,r_lg_account.default_type
         ,r_lg_account.default_date
         ,r_lg_account.default_amn default_amount
         ,r_lg_account.rq_default_type request_default_type
         ,DECODE (
             r_lg_account.rq_default_type
            ,'1', 'Total Forfeit Request'
            ,'2', 'Partial Forfeit Request'
            ,'3', 'Partial no Delete Request'
            ,'No Request')
             request_default_type_name
         ,r_lg_account.rq_default_dt request_default_date
         ,r_lg_account.rq_default_amn request_default_amount
         ,r_lg_account.next_commiss_dt next_commission_date
         ,r_lg_account.lg_amount_bal balance_amount
         ,lg_amn_tolerance tolerance_amount
         ,r_lg_account.lg_initial_amn initial_amount
         ,r_lg_account.acc_limit_amn limit_amount
         ,depprofits.account_ser_num deposit_acct_key
         ,depprofits.account_number deposit_account_number
         ,r_lg_account.dep_acc_cd deposit_account_cd
         ,r_lg_account.url_com_flg unrealised_commiss_flag
         ,DECODE (r_lg_account.url_com_flg, '1', 'Realised', 'Unrealised')
             unrealised_commiss_flag_name
         ,r_lg_account.stop_commission_dt stop_commission_date
         ,r_lg_account.stop_comm_usr stop_commission_user_code
         ,r_lg_account.fk_lgfk_productid product_id
         ,product.description product_description
         ,r_lg_account.fk_lg_beneficiacod beneficiary_cust_id
         ,bencust.c_digit beneficiary_cust_cd
         ,bencust.name_standard beneficiary_cust_name
         ,agree.account_ser_num agreement_acct_key
         ,agree.account_number agreement_account_number
         ,agree.account_cd agreement_account_cd
         ,r_lg_account.fk_unitcode unit_code
         ,r_lg_account.fkcurr_charges charges_currency_id
         ,charg.short_descr charges_currency
         ,r_lg_account.fkcust_issuer issue_customer_id
         ,r_lg_account.fkusr_modif_lgacc modification_user_code
         ,TRIM (modif.first_name) || ' ' || modif.last_name
             modification_user_name
         ,r_lg_account.fkcurr_moves movement_currency_id
         ,moves.short_descr movement_currency
         ,r_lg_account.fk_open_unit open_unit_code
         ,ope.unit_name open_unit_name
         ,r_lg_account.fk_print_unit print_unit_code
         ,pri.unit_name print_unit_name
         ,r_lg_account.fkcurr_limits limits_currency_id
         ,limits.short_descr limits_currency
         ,r_lg_account.issuance_justific issuance_justification
         ,  (  r_loan_account.nrm_cap_bal
             + r_loan_account.nrm_com_bal
             + r_loan_account.nrm_exp_bal
             + r_loan_account.nrm_acr_int_bal
             + r_loan_account.nrm_rl_int_bal
             + r_loan_account.nrm_url_int_bal
             + r_loan_account.ov_cap_bal
             + r_loan_account.ov_com_bal
             + r_loan_account.ov_exp_bal
             + r_loan_account.ov_rl_nrm_int_bal
             + r_loan_account.ov_rl_pnl_int_bal
             + r_loan_account.ov_url_nrm_int_bal
             + r_loan_account.ov_url_pnl_int_bal
             + a.nrm_accrual_amn
             + a.ov_accrual_amn
             + a.dormant_amn
             + a.unclear_amn)
          * -1
             gross_total
         ,  (  r_loan_account.nrm_cap_bal
             + r_loan_account.nrm_com_bal
             + r_loan_account.nrm_exp_bal
             + r_loan_account.nrm_acr_int_bal
             + r_loan_account.nrm_rl_int_bal
             + r_loan_account.nrm_url_int_bal
             + r_loan_account.ov_cap_bal
             + r_loan_account.ov_com_bal
             + r_loan_account.ov_exp_bal
             + r_loan_account.ov_rl_nrm_int_bal
             + r_loan_account.ov_rl_pnl_int_bal
             + r_loan_account.ov_url_nrm_int_bal
             + r_loan_account.ov_url_pnl_int_bal
             + a.nrm_accrual_amn
             + a.ov_accrual_amn
             + a.dormant_amn
             + a.unclear_amn)
          * -1
          * NVL (fr.rate, 1)
             gross_total_lcy
         ,NVL (fr.rate, 1) fixing_rate
         ,r_lg_account.lg_amount_bal * NVL (fr.rate, 1) balance_amount_lcy
   FROM   bank_parameters bp
          INNER JOIN r_lg_account ON (1 = 1)
          INNER JOIN lg
             ON lg.fk_productid_produ = r_lg_account.fk_lgfk_productid
          INNER JOIN profits_account
             ON     profits_account.lg_acc_sn = r_lg_account.acc_sn
                AND profits_account.prft_system = '14'
          LEFT JOIN w_stg_customer cust
             ON profits_account.cust_id = cust.cust_id
          LEFT JOIN generic_detail lgtyp
             ON     lgtyp.fk_generic_headpar = 'LGTYP'
                AND lgtyp.serial_num = acc_type
          LEFT JOIN generic_detail langu
             ON     langu.fk_generic_headpar = 'LANGU'
                AND langu.serial_num = language_code
          LEFT JOIN profits_account depprofits
             ON     depprofits.dep_acc_number = r_lg_account.dep_acc_sn
                AND depprofits.prft_system = 3
                AND depprofits.dep_acc_number > 0
          LEFT JOIN product
             ON r_lg_account.fk_lgfk_productid = product.id_product
          LEFT JOIN w_stg_customer bencust
             ON profits_account.cust_id = bencust.cust_id
          LEFT JOIN bankemployee modif
             ON (r_lg_account.fkusr_modif_lgacc = modif.id)
          LEFT JOIN profits_account agree
             ON     r_lg_account.fk_agreementfk_uni = agree.agr_unit
                AND r_lg_account.fk_agreementagr_ye = agree.agr_year
                AND r_lg_account.fk_agreementagr_sn = agree.agr_sn
                AND r_lg_account.fk_agreementagr_me = agree.agr_membership_sn
                AND r_lg_account.fk_agreementagr_sn > 0
                AND agree.prft_system = 19
          LEFT JOIN currency charg
             ON r_lg_account.fkcurr_charges = charg.id_currency
          LEFT JOIN currency moves
             ON r_lg_account.fkcurr_moves = moves.id_currency
          LEFT JOIN currency limits
             ON r_lg_account.fkcurr_limits = limits.id_currency
          LEFT JOIN unit ope ON r_lg_account.fk_open_unit = ope.code
          LEFT JOIN unit pri ON r_lg_account.fk_print_unit = pri.code
          LEFT JOIN r_loan_account
             ON     r_lg_account.lns_sn = r_loan_account.acc_sn
                AND r_lg_account.lns_type = r_loan_account.acc_type
                AND r_lg_account.lns_unit = r_loan_account.fk_unitcode
                AND r_loan_account.acc_sn > 0
          LEFT JOIN r_loan_account_inf a
             ON (    r_lg_account.lns_unit = a.fk_loan_accountfk
                 AND r_lg_account.lns_type = a.fk0loan_accountacc
                 AND r_lg_account.lns_sn = a.fk_loan_accountacc)
          LEFT JOIN w_eom_fixing_rate fr
             ON (    fr.eom_date = bp.scheduled_date
                 AND fr.currency_id = r_loan_account.fkcur_is_moved_in);
END;

