CREATE FUNCTION PROFITS.GL_PERIOD(DATE_IN DATE)
   RETURNS VARCHAR(50)
  LANGUAGE SQL
BEGIN
   DECLARE retval VARCHAR(50);
      SELECT MAX (period_id) || '-' || MAX (year0)
        INTO retval
        FROM glg_period
       WHERE (SELECT ADD_MONTHS (LAST_DAY (TO_DATE), -1)
                FROM glg_period
               WHERE date_in >= from_date AND date_in < TO_DATE
              UNION
              SELECT TO_DATE
                FROM glg_period
               WHERE date_in = TO_DATE) BETWEEN from_date
                                            AND TO_DATE;
return retval;
END;

