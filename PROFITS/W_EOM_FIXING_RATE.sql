create table W_EOM_FIXING_RATE
(
    EOM_DATE               DATE    not null,
    ACTIVATION_DATE        DATE,
    CURRENCY_ID            INTEGER not null,
    CURRENCY_CODE          CHAR(5),
    MULTIPLIER             INTEGER,
    RATE                   DECIMAL(12, 6),
    FIXING_TIMESTAMP       DATE,
    REVERSE_RATE           DECIMAL(12, 6),
    DOMESTIC_CURRENCY_FLAG VARCHAR(8),
    ACTIVATION_TIME        TIME,
    constraint PK_W_EOM_FIXING_RATE
        primary key (EOM_DATE, CURRENCY_ID)
);

CREATE PROCEDURE W_EOM_FIXING_RATE ( )
  SPECIFIC SQL160620112632541
  LANGUAGE SQL
  NOT DETERMINISTIC
  EXTERNAL ACTION
  MODIFIES SQL DATA
  CALLED ON NULL INPUT
  INHERIT SPECIAL REGISTERS
  OLD SAVEPOINT LEVEL
BEGIN
DELETE w_eom_fixing_rate;
INSERT INTO w_eom_fixing_rate (
               eom_date
              ,currency_id
              ,activation_date
              ,currency_code
              ,activation_time
              ,multiplier
              ,rate
              ,fixing_timestamp
              ,reverse_rate
             ,domestic_currency_flag)
   SELECT   cr.date_id eom_date
           ,fr.fk_currencyid_curr currency_id
           ,MAX (fr.activation_date) activation_date
           ,cur.short_descr currency_code
           ,MAX (activation_time) activation_time
           ,MAX (fr.multiplier) multiplier
           ,MAX (rate) rate
           ,MAX (fr.tmstamp) fixing_timestamp
           ,DECODE (
               short_descr
              ,b.domestic_currency, 1
              ,DECODE (NVL (MAX (rate), 0.0), 0.0, 0.0, 1.0 / MAX (rate)))
               reverse_rate
           ,DECODE (short_descr, domestic_currency, 'Domestic', 'Foreign')
               domestic_currency_flag
   FROM     (SELECT   c.date_id
                     ,r.id_currency
                     ,MAX (x.activation_date) activation_date
             FROM     fixing_rate x
                      FULL OUTER JOIN calendar c
                         ON x.activation_date <= c.date_id
                     ,currency r
             WHERE    x.fk_currencyid_curr = r.id_currency
             GROUP BY c.date_id, r.id_currency) cr
           ,(SELECT   x.fk_currencyid_curr
                     ,x.activation_date
                     ,x.multiplier
                     ,x.rate
                     ,x.tmstamp
                     ,MAX (x.activation_time) activation_time
             FROM     fixing_rate x
             GROUP BY x.fk_currencyid_curr
                     ,x.activation_date
                     ,x.multiplier
                     ,x.rate
                     ,x.tmstamp) fr
           ,currency cur
           ,bank_parameters b
   WHERE        cr.activation_date = fr.activation_date
            AND fr.fk_currencyid_curr = cr.id_currency
            AND cur.id_currency = fr.fk_currencyid_curr
   GROUP BY cr.date_id
           ,fr.fk_currencyid_curr
           ,cur.short_descr
           ,b.domestic_currency;
END;

