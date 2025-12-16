create table EOM_CURRENCY
(
    EOM_DATE         DATE    not null,
    ID_CURRENCY      INTEGER not null,
    ISO_CODE         SMALLINT,
    EURO_LOCKED_RATE DECIMAL(12, 6),
    EURO_IDENTIFIER  CHAR(1),
    ENTRY_STATUS     CHAR(1),
    NATIONAL_FLAG    CHAR(1),
    SHORT_DESCR      CHAR(5),
    DESCRIPTION      VARCHAR(40),
    constraint PK_EOM_CURRENCY
        primary key (ID_CURRENCY, EOM_DATE)
);

CREATE PROCEDURE EOM_CURRENCY ( )
  SPECIFIC SQL160620112636368
  LANGUAGE SQL
  NOT DETERMINISTIC
  EXTERNAL ACTION
  MODIFIES SQL DATA
  CALLED ON NULL INPUT
  INHERIT SPECIAL REGISTERS
  OLD SAVEPOINT LEVEL
BEGIN
DELETE eom_currency
WHERE  eom_date = (SELECT scheduled_date FROM bank_parameters);
INSERT INTO eom_currency (
               eom_date
              ,id_currency
              ,iso_code
              ,euro_locked_rate
              ,euro_identifier
              ,entry_status
              ,national_flag
              ,short_descr
              ,description)
   SELECT (SELECT scheduled_date FROM bank_parameters) AS eom_date
         ,id_currency
         ,iso_code
         ,euro_locked_rate
         ,euro_identifier
         ,entry_status
         ,national_flag
         ,short_descr
         ,description
   FROM   currency;
END;

