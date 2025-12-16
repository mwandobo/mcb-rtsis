create table W_EOM_LC_ACCOUNT
(
    EOM_DATE                      DATE,
    ACCT_KEY                      DECIMAL(11) not null,
    LC_ACCOUNT_CODE               CHAR(40)    not null,
    LC_ACCOUNT_CD                 SMALLINT,
    TRADE_TYPE_IND                CHAR(1),
    TRADE_TYPE_IND_NAME           VARCHAR(8),
    TF_ACCOUNT                    CHAR(40)    not null,
    TF_ACCOUNT_CD                 SMALLINT,
    REFERENCE_NO                  VARCHAR(40),
    ISSUER_NAME                   CHAR(70),
    BENEFICIARY_NAME              CHAR(70),
    LC_PRODUCT_CODE               INTEGER,
    LC_PRODUCT_DESCRIPTION        VARCHAR(40),
    OPENING_UNIT_CODE             INTEGER,
    OPENING_UNIT_NAME             VARCHAR(40),
    MONITORING_UNIT_CODE          INTEGER,
    MONITORING_UNIT_NAME          VARCHAR(40),
    JUSTIFICATION_CODE            INTEGER,
    JUSTIFICATION_DESCRIPTION     VARCHAR(40),
    LIMIT                         DECIMAL(15, 2),
    LIMIT_CURRENCY                CHAR(5),
    CURRENT_AMOUNT                DECIMAL(15, 2),
    INITIAL_AMOUNT                DECIMAL(15, 2),
    UTILIZED_AMOUNT               DECIMAL(15, 2),
    NON_UTILIZED_AMOUNT           DECIMAL(15, 2),
    CHARGES_CURRENCY              CHAR(5),
    OPENING_DATE                  DATE,
    ISSUE_DATE                    DATE,
    EXPIRATION_DATE               DATE,
    CLOSING_LOCATION              CHAR(20),
    SETTLEMENT_TYPE_IND           SMALLINT,
    SETTLEMENT_TYPE_IND_NAME      VARCHAR(16),
    DISPATCH_TYPE_IND             SMALLINT,
    DISPATCH_TYPE_IND_NAME        VARCHAR(5),
    LC_TYPE                       SMALLINT,
    LC_TYPE_NAME                  VARCHAR(19),
    TOLERANCE                     DECIMAL(7, 4),
    INVOICE_PERCENTAGE            DECIMAL(7, 4),
    CHARGING_FREQUENCY            SMALLINT,
    CHARGING_FREQUENCY_IND        CHAR(1),
    CHARGING_FREQUENCY_IND_NAME   VARCHAR(6),
    CONFIRM_FLAG                  CHAR(1),
    CONFIRM_FLAG_NAME             VARCHAR(13),
    IRREVOCABLE_FLAG              CHAR(1),
    IRREVOCABLE_FLAG_NAME         VARCHAR(17),
    PARTIAL_SHIPMENT_FLAG         CHAR(1),
    PARTIAL_SHIPMENT_FLAG_NAME    VARCHAR(20),
    TRANSHIPMENT_FLAG             CHAR(1),
    TRANSHIPMENT_FLAG_NAME        VARCHAR(16),
    ASSIGNED_OF_PROCEED_FLAG      CHAR(1),
    ASSIGNED_OF_PROCEED_FLAG_NAME VARCHAR(21),
    RED_CLAUSE_FLAG               CHAR(1),
    RED_CLAUSE_FLAG_NAME          VARCHAR(14),
    REVOLVING_FLAG                CHAR(1),
    REVOLVING_FLAG_NAME           VARCHAR(13),
    COLLECTING_BANK_BIC           CHAR(11),
    COLLECTING_BANK_NAME          VARCHAR(105),
    REMITTING_BANK_BIC            CHAR(11),
    REMITTING_BANK_NAME           VARCHAR(105),
    INVOICE_TYPE                  INTEGER,
    INVOICE_TYPE_DESCRIPTION      VARCHAR(40),
    AGREEMENT_NO                  CHAR(40),
    AGREEMENT_NO_CD               SMALLINT,
    AGREEMENT_LIMIT               DECIMAL(15, 2),
    UTILISED_LIMIT                DECIMAL(15, 2),
    NON_UTILISED_AMOUNT           DECIMAL(15, 2),
    TRANSFER_LC                   CHAR(40),
    STATUS_IND                    CHAR(1),
    STATUS_IND_NAME               VARCHAR(7),
    LOAN_ACCT_KEY                 DECIMAL(11),
    COMMISSION_AMOUNT             DECIMAL(15, 2),
    EXPENSES_AMOUNT               DECIMAL(15, 2),
    BALANCE_AMOUNT_LCY            DECIMAL(15, 2),
    FIXING_RATE                   DECIMAL(12, 6),
    PRINCIPAL_CUST_ID             INTEGER,
    PRINCIPAL_CUST_CD             SMALLINT,
    LOAN_CUST_ID                  INTEGER,
    LOAN_CUST_CD                  INTEGER,
    LOAN_CUSTOMER_NAME            VARCHAR(300),
    LOAN_OFFICER_ID               VARCHAR(8),
    LOAN_OFFICER_NAME             VARCHAR(300),
    LOAN_ACCOUNT_NO               VARCHAR(50),
    TOT_COMMISSION_BALANCE        DECIMAL(15, 2)
);

create unique index PK_W_EOM_LC_ACCOUNT
    on W_EOM_LC_ACCOUNT (EOM_DATE, ACCT_KEY);

CREATE PROCEDURE W_EOM_LC_ACCOUNT ( )
  SPECIFIC SQL160620112706983
  LANGUAGE SQL
  NOT DETERMINISTIC
  EXTERNAL ACTION
  MODIFIES SQL DATA
  CALLED ON NULL INPUT
  INHERIT SPECIAL REGISTERS
  OLD SAVEPOINT LEVEL
BEGIN
DELETE w_eom_lc_account
WHERE  eom_date = (SELECT scheduled_date FROM bank_parameters);
INSERT INTO w_eom_lc_account (
               eom_date
              ,acct_key
              ,lc_account_code
              ,lc_account_cd
              ,trade_type_ind
              ,trade_type_ind_name
              ,tf_account
              ,tf_account_cd
              ,reference_no
              ,issuer_name
              ,beneficiary_name
              ,lc_product_code
              ,lc_product_description
              ,opening_unit_code
              ,opening_unit_name
              ,monitoring_unit_code
              ,monitoring_unit_name
              ,justification_code
              ,justification_description
              ,LIMIT
              ,limit_currency
              ,current_amount
              ,initial_amount
              ,utilized_amount
              ,non_utilized_amount
              ,charges_currency
              ,opening_date
              ,issue_date
              ,expiration_date
              ,closing_location
              ,settlement_type_ind
              ,settlement_type_ind_name
              ,dispatch_type_ind
              ,dispatch_type_ind_name
              ,lc_type
              ,lc_type_name
              ,tolerance
              ,invoice_percentage
              ,charging_frequency
             ,charging_frequency_ind
              ,charging_frequency_ind_name
              ,confirm_flag
              ,confirm_flag_name
              ,irrevocable_flag
              ,irrevocable_flag_name
              ,partial_shipment_flag
              ,partial_shipment_flag_name
              ,transhipment_flag
              ,transhipment_flag_name
              ,assigned_of_proceed_flag
              ,assigned_of_proceed_flag_name
              ,red_clause_flag
              ,red_clause_flag_name
              ,revolving_flag
              ,revolving_flag_name
              ,collecting_bank_bic
              ,collecting_bank_name
              ,remitting_bank_bic
              ,remitting_bank_name
              ,invoice_type
              ,invoice_type_description
              ,agreement_no
              ,agreement_no_cd
              ,agreement_limit
              ,utilised_limit
              ,non_utilised_amount
              ,transfer_lc
              ,status_ind
              ,status_ind_name
              ,loan_acct_key
              ,commission_amount
              ,expenses_amount
              ,fixing_rate
              ,balance_amount_lcy)
   SELECT (SELECT scheduled_date FROM bank_parameters) eom_date
         ,pa.account_ser_num acct_key
         ,lc.lc_account_number lc_account_code
         ,lc.lc_acc_cd lc_account_cd
         ,plc.trade_type trade_type_ind
         ,DECODE (plc.trade_type, '3', 'Incoming', 'Outgoing')
             trade_type_ind_name
         ,tf.tf_number tf_account
         ,tf.tf_cd tf_account_cd
         ,tf.ref_no reference_no
         ,tf.prinicipal_name issuer_name
         ,tf.beneficiary_name beneficiary_name
         ,prd.id_product lc_product_code
         ,prd.description lc_product_description
         ,op.code opening_unit_code
         ,op.unit_name opening_unit_name
         ,mon.code monitoring_unit_code
         ,mon.unit_name monitoring_unit_name
         ,j.id_justific justification_code
         ,j.description justification_description
         ,lc.lc_amount LIMIT
         ,LIMIT.short_descr limit_currency
         ,lc.lc_total_issue_amn current_amount
         ,lc.lc_first_issue_amn initial_amount
         ,lc.lc_utilized_amn utilized_amount
         ,lc.lc_total_issue_amn - lc.lc_utilized_amn non_utilized_amount
         ,crg.short_descr charges_currency
         ,lc.opening_date opening_date
         ,lc.issue_date issue_date
         ,lc.expiry_date expiration_date
         ,lc.expiry_place closing_location
         ,lc.settlement_mode settlement_type_ind
         ,DECODE (
             lc.settlement_mode
            ,1, 'At Sight'
            ,2, 'By Acceptance'
            ,3, 'Defered'
            ,4, 'By Negotiation'
            ,5, 'By Mixed Payment'
            ,'n/a')
             settlement_type_ind_name
         ,lc.transmission dispatch_type_ind
         ,DECODE (
             lc.transmission
            ,'3', 'SWIFT'
            ,'2', 'Mail'
            ,'1', 'Telex'
            ,'n/a')
             dispatch_type_ind_name
         ,lc_type
         ,DECODE (
             lc.lc_type
            ,'1', 'No Transfer Allowed'
            ,'2', 'Transferable'
            ,'3', 'Back To Back'
            ,'4', 'Stand By'
            ,'n/a')
             lc_type_name
         ,lc.tolerance tolerance
         ,lc.invoice_perc invoice_percentage
         ,lc.comm_chrg_frq charging_frequency
         ,lc.comm_chrg_frqt charging_frequency_ind
         ,DECODE (lc.comm_chrg_frqt,  '1', 'Months',  '2', 'Days',  'n/a')
             charging_frequency_ind_name
         ,lc.confirm_flg confirm_flag
         ,DECODE (lc.confirm_flg, '1', 'Confirmed', 'Non-confirmed')
             confirm_flag_name
         ,lc.irrevocable_flag irrevocable_flag
         ,DECODE (
             lc.irrevocable_flag
            ,'1', 'Irrecoverable'
            ,'Non-irrecoverable')
             irrevocable_flag_name
         ,lc.partial_shipmnt_flag partial_shipment_flag
         ,DECODE (
             lc.partial_shipmnt_flag
            ,'1', 'Partial Shipment'
            ,'Non-partial Shipment')
             partial_shipment_flag_name
         ,lc.transhipmnt_flag transhipment_flag
         ,DECODE (
             lc.transhipmnt_flag
            ,'1', 'Transhipment'
            ,'Non-transhipment')
             transhipment_flag_name
         ,lc.assign_of_proceed assigned_of_proceed_flag
         ,DECODE (
             lc.assign_of_proceed
            ,'1', 'Assign Of Proceed'
            ,'Non-assign Of Proceed')
             assigned_of_proceed_flag_name
         ,lc.red_clause red_clause_flag
         ,DECODE (lc.red_clause, '1', 'Red Clause', 'Non-red Clause')
             red_clause_flag_name
         ,lc.revolving_ind revolving_flag
         ,DECODE (lc.revolving_ind, '1', 'Revolving', 'Non-Revolving')
             revolving_flag_name
         ,coll.bic collecting_bank_bic
         ,coll.bank_descr collecting_bank_name
         ,re.bic remitting_bank_bic
         ,re.bank_descr remitting_bank_name
         ,inv.serial_num invoice_type
         ,inv.description invoice_type_description
         ,paa.account_number agreement_no
         ,paa.account_cd agreement_no_cd
         ,agr.agr_limit agreement_limit
         ,agr.agr_utilised_limit utilised_limit
         ,agr.agr_limit - agr.agr_utilised_limit non_utilised_amount
         ,lc.transfer_lc_acc transfer_lc
         ,lc.lc_acc_status status_ind
         ,DECODE (
             lc.lc_acc_status
            ,'0', 'Deleted'
            ,'1', 'Pending'
            ,'2', 'Issued'
            ,'3', 'Closed'
            ,'n/a')
             status_ind_name
         ,lns.account_ser_num loan_acct_key
         ,l.tot_commission_amn commission_amount
         ,l.tot_expense_amn expenses_amount
         ,NVL (fr.rate, 1) fixing_rate
         ,lc_amount * NVL (fr.rate, 1) balance_amount_lcy
   FROM   bank_parameters bp
          INNER JOIN lc_account lc ON (1 = 1)
          INNER JOIN trade plc
             ON lc.fk_tradefk_product = plc.fk_prdid_product
          INNER JOIN product prd ON prd.id_product = lc.fk_tradefk_product
          INNER JOIN trade_finance tf ON lc.fk_trade_finance = tf.tf_number
          INNER JOIN profits_account pa
             ON     lc.lc_account_number = pa.account_number
                AND pa.prft_system = 39
          INNER JOIN unit op ON pa.lg_open_unit = op.code
          INNER JOIN unit mon ON pa.monotoring_unit = mon.code
          INNER JOIN justific j ON lc.fk_justificid_just = j.id_justific
          INNER JOIN currency LIMIT
             ON lc.fk_currencyid_curr = LIMIT.id_currency
          INNER JOIN currency crg ON lc.fk0currencyid_curr = crg.id_currency
          LEFT JOIN swift_allnce_bics coll
             ON tf.fk0swift_allncebic = coll.bic
          LEFT JOIN swift_allnce_bics re ON tf.fk_swift_allncebic = re.bic
          LEFT JOIN generic_detail inv
             ON     lc.fk_generic_detafk = inv.fk_generic_headpar
                AND lc.fk_generic_detaser = inv.serial_num
          LEFT JOIN agreement agr
             ON     lc.fk_agr_fk_unitcode = agr.fk_unitcode
                AND lc.fk_agreem_agr_year = agr.agr_year
                AND lc.fk_agreem_memb_sn = agr.agr_membership_sn
                AND lc.fk_agreementagr_sn = agr.agr_sn
          LEFT JOIN profits_account paa
             ON     paa.agr_unit = agr.fk_unitcode
                AND paa.agr_membership_sn = agr.agr_membership_sn
                AND paa.agr_year = agr.agr_year
                AND paa.agr_sn = agr.agr_sn
                AND paa.prft_system = 19
          LEFT JOIN profits_account lns
             ON (    lc.lns_type = lns.lns_type
                 AND lc.lns_unit = lns.lns_open_unit
                 AND lc.lns_sn = lns.lns_sn)
          LEFT JOIN r_loan_account l
             ON (    lc.lns_unit = l.fk_unitcode
                 AND lc.lns_type = l.acc_type
                 AND lc.lns_sn = l.acc_sn)
          LEFT JOIN w_eom_fixing_rate fr
             ON (    fr.eom_date = bp.scheduled_date
                 AND fr.currency_id = lc.fk_currencyid_curr)
   WHERE  plc.trade_type IN ('3', '4');
END;

