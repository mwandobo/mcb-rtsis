create table W_EOM_DOCUMENTARY_COLLECTION
(
    EOM_DATE                  DATE        not null,
    ACCT_KEY                  DECIMAL(11) not null,
    TRADE_FINANCE_CODE        VARCHAR(40),
    TRADE_FINANCE_CD          SMALLINT,
    PRODUCT_CODE              INTEGER,
    PRODUCT_DESCR             VARCHAR(40),
    TRADE_TYPE                VARCHAR(1),
    TRADE_TYPE_NAME           VARCHAR(7),
    OPENING_UNIT              INTEGER,
    OPENING_NAME              VARCHAR(40),
    MONITORING_UNIT           INTEGER,
    MONITORING_NAME           VARCHAR(40),
    COLLECTING_BIC            VARCHAR(11),
    COLLECTING_BANK_NAME      VARCHAR(105),
    REMITTING_BANK            VARCHAR(11),
    REMITTING_BANK_NAME       VARCHAR(105),
    COUNTRY_ORIGIN_CODE       INTEGER,
    COUNTRY_ORIGIN_DESCR      VARCHAR(40),
    REFERENCE_NUMBER          VARCHAR(40),
    AMOUNT                    DECIMAL(15, 2),
    CURRENCY                  VARCHAR(5),
    CENTRAL_BANK_CODE         INTEGER,
    CENTRAL_BANK_CODE_DESCR   VARCHAR(40),
    SETTLEMENT_TYPE           SMALLINT,
    SETTLEMENT_TYPE_NAME      VARCHAR(16),
    ISSUER_CODE               INTEGER,
    ISSUER_CD                 SMALLINT,
    ISSUER_SURNAME            VARCHAR(70),
    ISSUER_ADDRESS            VARCHAR(40),
    ISSUER_ACCOUNT_NUM        VARCHAR(40),
    ISSUER_ACCOUNT_CD         SMALLINT,
    BENEFICIARY_CODE          INTEGER,
    BENEFICIARY_CD            SMALLINT,
    BENEFICIARY_NAME          VARCHAR(70),
    BENEFICIARY_ADDRESS       VARCHAR(40),
    BENEFICIARY_ACCOUNT_NUM   VARCHAR(40),
    BENEFICIARY_ACCOUNT_CD    SMALLINT,
    CORRESPONDENT_CODE        INTEGER,
    CORRESPONDENT_CD          SMALLINT,
    CORRESPODENT_NAME         VARCHAR(70),
    CORRESPONDENT_ACCOUNT_NUM VARCHAR(40),
    CORRESPONDENT_ACCOUNT_CD  SMALLINT,
    STATUS                    VARCHAR(1),
    STATUS_NAME               VARCHAR(7),
    constraint PK_W_EOM_DOCUMENTARY_COLLECTIO
        primary key (EOM_DATE, ACCT_KEY)
);

CREATE PROCEDURE W_EOM_DOCUMENTARY_COLLECTION ( )
  SPECIFIC SQL160620112637182
  LANGUAGE SQL
  NOT DETERMINISTIC
  EXTERNAL ACTION
  MODIFIES SQL DATA
  CALLED ON NULL INPUT
  INHERIT SPECIAL REGISTERS
  OLD SAVEPOINT LEVEL
BEGIN
DELETE w_eom_documentary_collection
WHERE  eom_date = (SELECT scheduled_date FROM bank_parameters);
INSERT INTO w_eom_documentary_collection (
               eom_date
              ,acct_key
              ,trade_finance_code
              ,trade_finance_cd
              ,product_code
              ,product_descr
              ,trade_type
              ,trade_type_name
              ,opening_unit
              ,opening_name
              ,monitoring_unit
              ,monitoring_name
              ,collecting_bic
              ,collecting_bank_name
              ,remitting_bank
              ,remitting_bank_name
              ,country_origin_code
              ,country_origin_descr
              ,reference_number
              ,amount
              ,currency
              ,central_bank_code
              ,central_bank_code_descr
              ,settlement_type
              ,settlement_type_name
              ,issuer_code
              ,issuer_cd
              ,issuer_surname
              ,issuer_address
              ,issuer_account_num
              ,issuer_account_cd
              ,beneficiary_code
              ,beneficiary_cd
              ,beneficiary_name
              ,beneficiary_address
              ,beneficiary_account_num
              ,beneficiary_account_cd
              ,correspondent_code
              ,correspondent_cd
              ,correspodent_name
              ,correspondent_account_num
              ,correspondent_account_cd
              ,status
              ,status_name)
   SELECT (SELECT scheduled_date FROM bank_parameters) eom_date
         ,account_ser_num acct_key
         ,tf_number trade_finance_code
         ,tf_cd AS trade_finance_cd
         ,fk_tradefk_prod AS product_code
         ,p.description AS product_descr
         ,ptf.trade_type
         ,DECODE (ptf.trade_type, '1', 'Imports', 'Exports') trade_type_name
         ,op.code AS opening_unit
         ,op.unit_name AS opening_name
         ,mon.code AS monitoring_unit
         ,mon.unit_name AS monitoring_name
         ,coll.bic AS collecting_bic
         ,coll.bank_descr AS collecting_bank_name
         ,re.bic AS remitting_bank
         ,re.bank_descr AS remitting_bank_name
         ,cntry.serial_num AS country_origin_code
         ,cntry.description AS country_origin_descr
         ,t.ref_no AS reference_number
         ,t.tf_amount AS amount
         ,cur.short_descr AS currency
         ,cbc.serial_num AS central_bank_code
         ,cbc.description AS central_bank_code_descr
         ,t.settlement_mode AS settlement_type
         ,DECODE (
             t.settlement_mode
            ,1, 'At Sight'
            ,2, 'By Acceptance'
            ,3, 'Defered'
            ,4, 'By Negotiation'
            ,5, 'By Mixed Payment'
            ,'n/a')
             AS settlement_type_name
         ,isu.cust_id AS issuer_code
         ,isu.c_digit AS issuer_cd
         ,t.prinicipal_name AS issuer_surname
         ,t.principal_comm_addr AS issuer_address
         ,t.principal_acc_num AS issuer_account_num
         ,t.principal_acc_cd AS issuer_account_cd
         ,ben.cust_id AS beneficiary_code
         ,ben.c_digit AS beneficiary_cd
         ,t.beneficiary_name AS beneficiary_name
         ,t.benef_comm_addr AS beneficiary_address
         ,t.benef_acc_num AS beneficiary_account_num
         ,t.benef_acc_cd AS beneficiary_account_cd
         ,cor.cust_id AS correspondent_code
         ,cor.c_digit AS correspondent_cd
         ,t.correspondent_name AS correspodent_name
         ,t.corr_bank_acc_num AS correspondent_account_num
         ,t.corr_bank_acc_cd AS correspondent_account_cd
         ,t.status
         ,DECODE (
             t.status
            ,'0', 'Deleted'
            ,'1', 'Pending'
            ,'2', 'Issued'
            ,'3', 'Closed'
            ,'n/a')
             AS status_name
   FROM   trade_finance t
         ,profits_account
         ,product p
         ,unit op
         ,unit mon
         ,swift_allnce_bics coll
         ,trade ptf
         ,swift_allnce_bics re
         ,generic_detail cntry
         ,currency cur
         ,customer isu
         ,customer ben
         ,customer cor
         ,generic_detail cbc
   WHERE      profits_account.account_number = t.tf_number
          AND profits_account.prft_system = 37
          AND t.fk_tradefk_prod = p.id_product
          AND t.fk_unitcode = op.code
          AND t.fk0unitcode = mon.code
          AND t.fk0swift_allncebic = coll.bic
          AND t.fk_swift_allncebic = re.bic
          AND ptf.fk_prdid_product = t.fk_tradefk_prod
          AND t.fk0generic_detafk = cntry.fk_generic_headpar
          AND t.fk0generic_detaser = cntry.serial_num
          AND ptf.trade_type IN ('1', '2')
          AND t.fk_currencyid_curr = cur.id_currency
          AND t.fk0customercust_id = isu.cust_id
          AND t.fk_customercust_id = ben.cust_id
          AND t.fk1customercust_id = cor.cust_id
          AND t.fk_generic_detafk = cbc.fk_generic_headpar
          AND t.fk_generic_detaser = cbc.serial_num;
END;

