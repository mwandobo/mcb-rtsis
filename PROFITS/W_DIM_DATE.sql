create table W_DIM_DATE
(
    DATE_KEY               DECIMAL(10),
    FULL_DATE              DATE,
    DAY_NAME               CHAR(3),
    HOLIDAY_INDICATOR      VARCHAR(11),
    YEAR_MONTH             INTEGER,
    YEAR_MINUS_1_LAST_DATE DATE,
    PRODUCTION_DATE        DATE
);

create unique index PK_DIMDATE
    on W_DIM_DATE (FULL_DATE);

CREATE PROCEDURE W_DIM_DATE ( )
  SPECIFIC SQL160620112632239
  LANGUAGE SQL
  NOT DETERMINISTIC
  EXTERNAL ACTION
  MODIFIES SQL DATA
  CALLED ON NULL INPUT
  INHERIT SPECIAL REGISTERS
  OLD SAVEPOINT LEVEL
BEGIN
DELETE w_dim_date;
INSERT INTO w_dim_date (
               date_key
              ,full_date
              ,day_name
              ,holiday_indicator
              ,year_month
              ,year_minus_1_last_date
              ,production_date)
   WITH s
        AS (SELECT day_id, date_id
            FROM   calendar
            WHERE  holiday_ind = '0')
       ,t
        AS (SELECT c.*
                  ,TO_DATE (
                      (TO_NUMBER (TO_CHAR (date_id, 'rrrr')) - 1) || '-12-31'
                     ,'yyyy-mm-dd')
                      year_minus_1_last_date
                  , (SELECT   eom_date
                     FROM     eom_currency
                     WHERE    c.date_id = eom_date
                     GROUP BY eom_date)
                      production_date
            FROM   calendar c)
   SELECT TO_NUMBER (TO_CHAR (date_id, 'YYYYMMDD')) date_key
         ,date_id full_date
         ,day_name
         ,DECODE (holiday_ind, '0', 'Non-holiday', 'Holiday')
             holiday_indicator
         ,TO_NUMBER (TO_CHAR (date_id, 'YYYYMM')) year_month
         ,year_minus_1_last_date
         ,production_date
   FROM   t;
END;

