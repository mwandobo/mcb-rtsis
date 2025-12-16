create table W_EOM_ACCOUNT
(
    EOM_DATE           DATE,
    ACCT_KEY           DECIMAL(11),
    ACCOUNT_NUMBER     CHAR(40),
    PRFT_SYSTEM        SMALLINT,
    ACCOUNT_CD         SMALLINT,
    CRM_NOTES_KEY      DECIMAL(11) default 0,
    PROFILE_KEY        SMALLINT,
    AGREE_ACCT_KEY     DECIMAL(11),
    AGREE_PATHSTRING   VARCHAR(2000),
    BENEFICIARIES_NAME VARCHAR(4000),
    OPEN_DATE          DATE,
    MONITORING_UNIT    DECIMAL(5),
    CUST_ID            DECIMAL(7),
    CUST_CD            DECIMAL(1),
    ACCT_STATUS_IND    VARCHAR(10),
    PRODUCT_ID         DECIMAL(5),
    CURRENCY_CODE      VARCHAR(5),
    OPEN_USER_CODE     VARCHAR(8),
    DATE_CLOSED        DATE,
    CUSTOMER_NAME      VARCHAR(91),
    CLOSING_DATE       DATE
);

create unique index PK_W_EOM_ACCOUNT
    on W_EOM_ACCOUNT (EOM_DATE, ACCT_KEY);

CREATE PROCEDURE W_EOM_ACCOUNT ( )
  SPECIFIC SQL160620112636572
  LANGUAGE SQL
  NOT DETERMINISTIC
  EXTERNAL ACTION
  MODIFIES SQL DATA
  CALLED ON NULL INPUT
  INHERIT SPECIAL REGISTERS
  OLD SAVEPOINT LEVEL
BEGIN
DELETE w_eom_account
WHERE  eom_date = (SELECT scheduled_date FROM bank_parameters);
INSERT INTO w_eom_account (
               eom_date
              ,acct_key
              ,account_number
              ,prft_system
              ,account_cd
              ,beneficiaries_name
              ,open_date
              ,monitoring_unit
              ,cust_id
              ,cust_cd
              ,acct_status_ind
              ,product_id
              ,currency_code
              ,open_user_code
              ,date_closed
              ,customer_name
              ,closing_date)
SELECT   (SELECT scheduled_date FROM bank_parameters)
        ,account_ser_num acct_key
        ,b.account_number
        ,prft_system
        ,account_cd
        ,LISTAGG (TRIM (cust.name_standard), ', ')
            WITHIN GROUP (ORDER BY d.main_benef_flg DESC, cust.name_standard)
            beneficiaries_name
        ,COALESCE (r_deposit_account.opening_date, r_loan_account.acc_open_dt)
            open_date
        ,b.monotoring_unit monitoring_unit
        ,b.cust_id
        ,b.c_digit cust_cd
        ,CASE
            WHEN r_deposit_account.entry_status = '6'
            THEN
               'Dormant'
            WHEN r_deposit_account.entry_status = '4'
            THEN
               'Cancelled'
            WHEN    r_deposit_account.entry_status IN ('0', '3')
                 OR r_loan_account.acc_status = '3'
                 OR c.agr_status = '4'
            THEN
               'Closed'
            ELSE
               'Effective'
         END
            acct_status_ind
        ,product_id
        ,ccy.short_descr currency_code
        ,COALESCE (r_deposit_account.fk_usrcode, r_loan_account.usr)
            open_user_code
        ,GREATEST (
            NVL(DECODE (r_loan_account.acc_status, '3', lst_trx_dt), DATE '0001-01-01')
           ,NVL(r_deposit_account.closing_date, DATE '0001-01-01')
           )
            date_closed
        ,c02.name_standard customer_name
        ,DECODE (
            GREATEST (
               NVL (r_deposit_account.closing_date, DATE '0001-01-01')
              ,NVL (c.agr_expiry_dt, DATE '0001-01-01')
              ,NVL (r_loan_account.acc_exp_dt, DATE '0001-01-01'))
           ,DATE '0001-01-01', DATE '9999-12-31'
           ,GREATEST (
               NVL (r_deposit_account.closing_date, DATE '0001-01-01')
              ,NVL (c.agr_expiry_dt, DATE '0001-01-01')
              ,NVL (r_loan_account.acc_exp_dt, DATE '0001-01-01')))
            closing_date
FROM     profits_account b
         LEFT JOIN r_agreement c
            ON (    b.agr_unit = c.fk_unitcode
                AND b.agr_year = c.agr_year
                AND b.agr_sn = c.agr_sn
                AND b.agr_membership_sn = c.agr_membership_sn
                AND c.agr_status IN ('2', '3'))
         LEFT JOIN agreement_benef d
            ON (    c.fk_unitcode = d.fk_agreementfk_uni
                AND c.agr_year = d.fk_agreementagr_ye
                AND c.agr_sn = d.fk_agreementagr_sn
                AND c.agr_membership_sn = d.fk_agreementagr_me)
         LEFT JOIN w_stg_customer cust ON cust.cust_id = d.fk_customercust_id
         LEFT JOIN w_stg_customer c02 ON c02.cust_id = b.cust_id
         LEFT JOIN r_deposit_account
            ON     r_deposit_account.account_number = b.dep_acc_number
               AND b.prft_system = 3
         LEFT JOIN r_loan_account
            ON     b.lns_open_unit = r_loan_account.fk_unitcode
               AND b.lns_sn = r_loan_account.acc_sn
               AND b.lns_type = r_loan_account.acc_type
               AND b.prft_system = 4
         LEFT JOIN currency ccy ON ccy.id_currency = b.movement_currency
GROUP BY b.account_number
        ,b.account_cd
        ,b.prft_system
        ,b.account_ser_num
        ,b.agr_unit
        ,b.agr_membership_sn
        ,b.agr_sn
        ,b.agr_year
        ,r_deposit_account.opening_date
        ,COALESCE (
            r_deposit_account.opening_date
           ,r_loan_account.acc_open_dt)
        ,b.monotoring_unit
        ,b.cust_id
        ,b.c_digit
        ,CASE
            WHEN r_deposit_account.entry_status = '6'
            THEN
               'Dormant'
            WHEN r_deposit_account.entry_status = '4'
            THEN
               'Cancelled'
            WHEN    r_deposit_account.entry_status IN ('0', '3')
                 OR r_loan_account.acc_status = '3'
                 OR c.agr_status = '4'
            THEN
               'Closed'
            ELSE
               'Effective'
         END
        ,product_id
        ,ccy.short_descr
        ,COALESCE (r_deposit_account.fk_usrcode, r_loan_account.usr)
        ,GREATEST (
            NVL(DECODE (r_loan_account.acc_status, '3', lst_trx_dt), DATE '0001-01-01')
           ,NVL(r_deposit_account.closing_date, DATE '0001-01-01')
           )
        ,c02.name_standard
        ,DECODE (
            GREATEST (
               NVL (r_deposit_account.closing_date, DATE '0001-01-01')
              ,NVL (c.agr_expiry_dt, DATE '0001-01-01')
              ,NVL (r_loan_account.acc_exp_dt, DATE '0001-01-01'))
           ,DATE '0001-01-01', DATE '9999-12-31'
           ,GREATEST (
               NVL (r_deposit_account.closing_date, DATE '0001-01-01')
              ,NVL (c.agr_expiry_dt, DATE '0001-01-01')
              ,NVL (r_loan_account.acc_exp_dt, DATE '0001-01-01')));
END;

