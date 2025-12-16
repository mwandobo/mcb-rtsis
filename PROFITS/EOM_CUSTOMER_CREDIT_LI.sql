create table EOM_CUSTOMER_CREDIT_LI
(
    EOM_DATE             DATE           not null,
    CRLINE_AMOUNT        DECIMAL(15, 2) not null,
    ENTRY_STATUS         CHAR(1),
    EVALUATION_DT        DATE,
    EXPIRY_DATE          DATE,
    FK_CURRENCYID_CURR   INTEGER        not null,
    FK_CUSTOMERCUST_ID   INTEGER        not null,
    FK_GENERIC_DETAFK    CHAR(5)        not null,
    FK_GENERIC_DETASER   INTEGER        not null,
    FK_UNITCODE          INTEGER,
    FK_USRCODE           CHAR(8),
    HISTORY_CNT          DECIMAL(10),
    INTRACOM_FLG         CHAR(1),
    REEVALUATION_DT      DATE,
    TMSTAMP              TIMESTAMP(6)   not null,
    UTILISED_AMOUNT      DECIMAL(15, 2),
    EURO_UTILISED_AMOUNT DECIMAL(15, 2),
    EURO_CRLINE_AMOUNT   DECIMAL(15, 2),
    FIXING_RATE          DECIMAL(12, 6),
    constraint PK_EOM_LI
        primary key (EOM_DATE, FK_CURRENCYID_CURR, FK_CUSTOMERCUST_ID, FK_GENERIC_DETAFK, FK_GENERIC_DETASER, TMSTAMP)
);

CREATE PROCEDURE EOM_CUSTOMER_CREDIT_LI ( )
  SPECIFIC SQL160620112636369
  LANGUAGE SQL
  NOT DETERMINISTIC
  EXTERNAL ACTION
  MODIFIES SQL DATA
  CALLED ON NULL INPUT
  INHERIT SPECIAL REGISTERS
  OLD SAVEPOINT LEVEL
BEGIN
DELETE eom_customer_credit_li
WHERE  eom_date = (SELECT scheduled_date FROM bank_parameters);
INSERT INTO eom_customer_credit_li (
               eom_date
              ,crline_amount
              ,entry_status
              ,evaluation_dt
              ,expiry_date
              ,fk_currencyid_curr
              ,fk_customercust_id
              ,fk_generic_detafk
              ,fk_generic_detaser
              ,fk_unitcode
              ,fk_usrcode
              ,history_cnt
              ,intracom_flg
              ,reevaluation_dt
              ,tmstamp
              ,utilised_amount
              ,euro_crline_amount
              ,euro_utilised_amount
              ,fixing_rate)
   SELECT (SELECT scheduled_date FROM bank_parameters) AS eom_date
         ,crline_amount
         ,entry_status
         ,evaluation_dt
         ,expiry_date
         ,fk_currencyid_curr
         ,fk_customercust_id
         ,fk_generic_detafk
         ,fk_generic_detaser
         ,fk_unitcode
         ,fk_usrcode
         ,history_cnt
         ,intracom_flg
         ,reevaluation_dt
         ,tmstamp
         ,utilised_amount
         ,crline_amount * rate AS euro_crline_amount
         ,utilised_amount * rate AS euro_utilised_amount
         ,d.reverse_rate AS fixing_rate
   FROM   customer_credit_li c
          INNER JOIN bank_parameters p ON 1 = 1
          LEFT JOIN w_eom_fixing_rate d
             ON     d.currency_id = c.fk_currencyid_curr
                AND d.eom_date = p.scheduled_date;
END;

