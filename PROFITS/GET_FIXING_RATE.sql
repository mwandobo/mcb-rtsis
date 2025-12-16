-- Cyclic dependencies found

CREATE FUNCTION GET_FIXING_RATE (
    INCURRENCYID	INTEGER,
    P_ACTIVATION_DATE	DATE DEFAULT NULL )
  RETURNS DECIMAL(12, 6)
  SPECIFIC SQL160728131621795
  NOT SECURED
  LANGUAGE SQL
  NOT DETERMINISTIC
  EXTERNAL ACTION
  READS SQL DATA
  INHERIT SPECIAL REGISTERS
BEGIN
   DECLARE v_rate                   DECIMAL(12,6);
   DECLARE v_activation_date        DATE;
   DECLARE v_id_currency_domestic   INTEGER;
   DECLARE v_curr_trx_date          DATE;
BEGIN
   SELECT   curr_trx_date, id_currency
     INTO   v_curr_trx_date, v_id_currency_domestic
     FROM   bank_parameters, currency
    WHERE   short_descr = domestic_currency;

--   IF v_id_currency_domestic = incurrencyid
--   THEN
--      RETURN 1;
--   END IF;

   SET v_activation_date = NVL (p_activation_date, v_curr_trx_date);

   SELECT   d.rate
     INTO   v_rate
     FROM   fixing_rate d
    WHERE   d.fk_currencyid_curr = incurrencyid
            AND d.activation_date =
                  (SELECT   MAX (pp.activation_date)
                     FROM   fixing_rate pp
                    WHERE   d.fk_currencyid_curr = pp.fk_currencyid_curr
                            AND pp.activation_date <= v_activation_date /*(SELECT   curr_trx_date FROM bank_parameters)*/
                                                                       )
            AND d.activation_time =
                  (SELECT   MAX (e.activation_time)
                     FROM   fixing_rate e
                    WHERE   d.fk_currencyid_curr = e.fk_currencyid_curr
                            AND d.activation_date = e.activation_date);

   SET v_rate = 1 / v_rate;
   RETURN v_rate;
END;
END
CREATE OR REPLACE FUNCTION PROFITS.GET_FIXING_RATE (
    INCURRENCYID  INTEGER,
    P_ACTIVATION_DATE   DATE DEFAULT NULL )
  RETURNS DECIMAL(12, 6)
BEGIN
   DECLARE v_rate                   DECIMAL(12,6);
   DECLARE v_activation_date        DATE;
   DECLARE v_id_currency_domestic   INTEGER;
   DECLARE v_curr_trx_date          DATE;
BEGIN
   SELECT   curr_trx_date, id_currency
     INTO   v_curr_trx_date, v_id_currency_domestic
     FROM   bank_parameters, currency
    WHERE   short_descr = domestic_currency;
--   IF v_id_currency_domestic = incurrencyid
--   THEN
--      RETURN 1;
--   END IF;
   SET v_activation_date = NVL (p_activation_date, v_curr_trx_date);
   SELECT   d.rate
     INTO   v_rate
     FROM   fixing_rate d
    WHERE   d.fk_currencyid_curr = incurrencyid
            AND d.activation_date =
                  (SELECT   MAX (pp.activation_date)
                     FROM   fixing_rate pp
                    WHERE   d.fk_currencyid_curr = pp.fk_currencyid_curr
                            AND pp.activation_date <= v_activation_date /*(SELECT   curr_trx_date FROM bank_parameters)*/
                                                                       )
            AND d.activation_time =
                  (SELECT   MAX (e.activation_time)
                     FROM   fixing_rate e
                    WHERE   d.fk_currencyid_curr = e.fk_currencyid_curr
                            AND d.activation_date = e.activation_date);
--  SET v_rate = 1 / v_rate;
   RETURN v_rate;
END;
END;

CREATE FUNCTION GET_FIXING_RATE (
    INCURRENCYID	INTEGER,
    P_ACTIVATION_DATE	DATE DEFAULT NULL )
  RETURNS DECIMAL(12, 6)
  SPECIFIC SQL160728131621795
  NOT SECURED
  LANGUAGE SQL
  NOT DETERMINISTIC
  EXTERNAL ACTION
  READS SQL DATA
  INHERIT SPECIAL REGISTERS
BEGIN
   DECLARE v_rate                   DECIMAL(12,6);
   DECLARE v_activation_date        DATE;
   DECLARE v_id_currency_domestic   INTEGER;
   DECLARE v_curr_trx_date          DATE;
BEGIN
   SELECT   curr_trx_date, id_currency
     INTO   v_curr_trx_date, v_id_currency_domestic
     FROM   bank_parameters, currency
    WHERE   short_descr = domestic_currency;

--   IF v_id_currency_domestic = incurrencyid
--   THEN
--      RETURN 1;
--   END IF;

   SET v_activation_date = NVL (p_activation_date, v_curr_trx_date);

   SELECT   d.rate
     INTO   v_rate
     FROM   fixing_rate d
    WHERE   d.fk_currencyid_curr = incurrencyid
            AND d.activation_date =
                  (SELECT   MAX (pp.activation_date)
                     FROM   fixing_rate pp
                    WHERE   d.fk_currencyid_curr = pp.fk_currencyid_curr
                            AND pp.activation_date <= v_activation_date /*(SELECT   curr_trx_date FROM bank_parameters)*/
                                                                       )
            AND d.activation_time =
                  (SELECT   MAX (e.activation_time)
                     FROM   fixing_rate e
                    WHERE   d.fk_currencyid_curr = e.fk_currencyid_curr
                            AND d.activation_date = e.activation_date);

   SET v_rate = 1 / v_rate;
   RETURN v_rate;
END;
END
CREATE OR REPLACE FUNCTION PROFITS.GET_FIXING_RATE (
    INCURRENCYID  INTEGER,
    P_ACTIVATION_DATE   DATE DEFAULT NULL )
  RETURNS DECIMAL(12, 6)
BEGIN
   DECLARE v_rate                   DECIMAL(12,6);
   DECLARE v_activation_date        DATE;
   DECLARE v_id_currency_domestic   INTEGER;
   DECLARE v_curr_trx_date          DATE;
BEGIN
   SELECT   curr_trx_date, id_currency
     INTO   v_curr_trx_date, v_id_currency_domestic
     FROM   bank_parameters, currency
    WHERE   short_descr = domestic_currency;
--   IF v_id_currency_domestic = incurrencyid
--   THEN
--      RETURN 1;
--   END IF;
   SET v_activation_date = NVL (p_activation_date, v_curr_trx_date);
   SELECT   d.rate
     INTO   v_rate
     FROM   fixing_rate d
    WHERE   d.fk_currencyid_curr = incurrencyid
            AND d.activation_date =
                  (SELECT   MAX (pp.activation_date)
                     FROM   fixing_rate pp
                    WHERE   d.fk_currencyid_curr = pp.fk_currencyid_curr
                            AND pp.activation_date <= v_activation_date /*(SELECT   curr_trx_date FROM bank_parameters)*/
                                                                       )
            AND d.activation_time =
                  (SELECT   MAX (e.activation_time)
                     FROM   fixing_rate e
                    WHERE   d.fk_currencyid_curr = e.fk_currencyid_curr
                            AND d.activation_date = e.activation_date);
--  SET v_rate = 1 / v_rate;
   RETURN v_rate;
END;
END;

