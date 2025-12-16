create table EOM_GLG_UNIT_TOTAL
(
    YEAR0              SMALLINT,
    FK_CURRENCYID_CURR INTEGER,
    FK_GLG_ACCOUNTACCO CHAR(21),
    FK_UNITCODE        INTEGER,
    AVER_BAL_SHEET     DECIMAL(15, 2),
    YR_AVR_BAL         DECIMAL(15, 2),
    DEBIT01            DECIMAL(15, 2),
    DEBIT02            DECIMAL(15, 2),
    DEBIT03            DECIMAL(15, 2),
    DEBIT04            DECIMAL(15, 2),
    DEBIT05            DECIMAL(15, 2),
    DEBIT06            DECIMAL(15, 2),
    DEBIT07            DECIMAL(15, 2),
    DEBIT08            DECIMAL(15, 2),
    DEBIT09            DECIMAL(15, 2),
    DEBIT10            DECIMAL(15, 2),
    DEBIT11            DECIMAL(15, 2),
    DEBIT12            DECIMAL(15, 2),
    CREDIT01           DECIMAL(15, 2),
    CREDIT02           DECIMAL(15, 2),
    CREDIT03           DECIMAL(15, 2),
    CREDIT04           DECIMAL(15, 2),
    CREDIT05           DECIMAL(15, 2),
    CREDIT06           DECIMAL(15, 2),
    CREDIT07           DECIMAL(15, 2),
    CREDIT08           DECIMAL(15, 2),
    CREDIT09           DECIMAL(15, 2),
    CREDIT10           DECIMAL(15, 2),
    CREDIT11           DECIMAL(15, 2),
    CREDIT12           DECIMAL(15, 2),
    AVER_BAL01         DECIMAL(15, 2),
    AVER_BAL02         DECIMAL(15, 2),
    AVER_BAL03         DECIMAL(15, 2),
    AVER_BAL04         DECIMAL(15, 2),
    AVER_BAL05         DECIMAL(15, 2),
    AVER_BAL06         DECIMAL(15, 2),
    AVER_BAL07         DECIMAL(15, 2),
    AVER_BAL08         DECIMAL(15, 2),
    AVER_BAL09         DECIMAL(15, 2),
    AVER_BAL10         DECIMAL(15, 2),
    AVER_BAL11         DECIMAL(15, 2),
    AVER_BAL12         DECIMAL(15, 2),
    MIN_BAL_01         DECIMAL(15, 2),
    MIN_BAL_02         DECIMAL(15, 2),
    MIN_BAL_03         DECIMAL(15, 2),
    MIN_BAL_04         DECIMAL(15, 2),
    MIN_BAL_05         DECIMAL(15, 2),
    MIN_BAL_06         DECIMAL(15, 2),
    MIN_BAL_07         DECIMAL(15, 2),
    MIN_BAL_08         DECIMAL(15, 2),
    MIN_BAL_09         DECIMAL(15, 2),
    MIN_BAL_10         DECIMAL(15, 2),
    MIN_BAL_11         DECIMAL(15, 2),
    MIN_BAL_12         DECIMAL(15, 2),
    MAX_BAL_01         DECIMAL(15, 2),
    MAX_BAL_02         DECIMAL(15, 2),
    MAX_BAL_03         DECIMAL(15, 2),
    MAX_BAL_04         DECIMAL(15, 2),
    MAX_BAL_05         DECIMAL(15, 2),
    MAX_BAL_06         DECIMAL(15, 2),
    MAX_BAL_07         DECIMAL(15, 2),
    MAX_BAL_08         DECIMAL(15, 2),
    MAX_BAL_09         DECIMAL(15, 2),
    MAX_BAL_10         DECIMAL(15, 2),
    MAX_BAL_11         DECIMAL(15, 2),
    MAX_BAL_12         DECIMAL(15, 2),
    BAL_SHEET_CR       DECIMAL(15, 2),
    BAL_SHEET_DB       DECIMAL(15, 2),
    MIN_BAL_SHEET      DECIMAL(15, 2),
    MAX_BAL_SHEET      DECIMAL(15, 2),
    INVENT_CR          DECIMAL(18, 2),
    INVENT_DB          DECIMAL(18, 2),
    LST_UPDAT_DATE     DATE,
    LEVEL0             CHAR(1),
    EOM_DATE           DATE,
    BALANCE            DECIMAL(18, 2) default 0
);

create unique index EOM_GLG_UNIT_TOTAL_PK
    on EOM_GLG_UNIT_TOTAL (EOM_DATE, YEAR0, FK_CURRENCYID_CURR, FK_GLG_ACCOUNTACCO, FK_UNITCODE);

CREATE PROCEDURE EOM_GLG_UNIT_TOTAL ( )
  SPECIFIC SQL160620112636470
  LANGUAGE SQL
  NOT DETERMINISTIC
  EXTERNAL ACTION
  MODIFIES SQL DATA
  CALLED ON NULL INPUT
  INHERIT SPECIAL REGISTERS
  OLD SAVEPOINT LEVEL
BEGIN
DELETE eom_glg_unit_total
WHERE  eom_date = (SELECT scheduled_date FROM bank_parameters);
INSERT INTO eom_glg_unit_total (
               year0
              ,fk_currencyid_curr
              ,fk_glg_accountacco
              ,fk_unitcode
              ,aver_bal_sheet
              ,yr_avr_bal
              ,debit01
              ,debit02
              ,debit03
              ,debit04
              ,debit05
              ,debit06
              ,debit07
              ,debit08
              ,debit09
              ,debit10
              ,debit11
              ,debit12
              ,credit01
              ,credit02
              ,credit03
              ,credit04
              ,credit05
              ,credit06
              ,credit07
              ,credit08
              ,credit09
              ,credit10
              ,credit11
              ,credit12
              ,aver_bal01
              ,aver_bal02
              ,aver_bal03
              ,aver_bal04
              ,aver_bal05
              ,aver_bal06
              ,aver_bal07
              ,aver_bal08
              ,aver_bal09
              ,aver_bal10
              ,aver_bal11
              ,aver_bal12
              ,min_bal_01
              ,min_bal_02
              ,min_bal_03
              ,min_bal_04
              ,min_bal_05
              ,min_bal_06
              ,min_bal_07
              ,min_bal_08
              ,min_bal_09
              ,min_bal_10
              ,min_bal_11
              ,min_bal_12
              ,max_bal_01
              ,max_bal_02
              ,max_bal_03
              ,max_bal_04
              ,max_bal_05
              ,max_bal_06
              ,max_bal_07
              ,max_bal_08
              ,max_bal_09
              ,max_bal_10
              ,max_bal_11
              ,max_bal_12
              ,bal_sheet_cr
              ,bal_sheet_db
              ,min_bal_sheet
              ,max_bal_sheet
              ,invent_cr
              ,invent_db
              ,lst_updat_date
              ,level0
              ,eom_date
              ,balance)
   SELECT year0
         ,fk_currencyid_curr
         ,fk_glg_accountacco
         ,fk_unitcode
         ,aver_bal_sheet
         ,yr_avr_bal
         ,debit01
         ,debit02
         ,debit03
         ,debit04
         ,debit05
         ,debit06
         ,debit07
         ,debit08
         ,debit09
         ,debit10
         ,debit11
         ,debit12
         ,credit01
         ,credit02
         ,credit03
         ,credit04
         ,credit05
         ,credit06
         ,credit07
         ,credit08
         ,credit09
         ,credit10
         ,credit11
         ,credit12
         ,aver_bal01
         ,aver_bal02
         ,aver_bal03
         ,aver_bal04
        ,aver_bal05
         ,aver_bal06
         ,aver_bal07
         ,aver_bal08
         ,aver_bal09
         ,aver_bal10
         ,aver_bal11
         ,aver_bal12
         ,min_bal_01
         ,min_bal_02
         ,min_bal_03
         ,min_bal_04
         ,min_bal_05
         ,min_bal_06
         ,min_bal_07
         ,min_bal_08
         ,min_bal_09
         ,min_bal_10
         ,min_bal_11
         ,min_bal_12
         ,max_bal_01
         ,max_bal_02
         ,max_bal_03
         ,max_bal_04
         ,max_bal_05
         ,max_bal_06
         ,max_bal_07
         ,max_bal_08
         ,max_bal_09
         ,max_bal_10
         ,max_bal_11
         ,max_bal_12
         ,bal_sheet_cr
         ,bal_sheet_db
         ,min_bal_sheet
         ,max_bal_sheet
         ,invent_cr
         ,invent_db
         ,lst_updat_date
         ,level0
         , (SELECT scheduled_date FROM bank_parameters) AS eom_date
         ,  (  credit01
             + credit02
             + credit03
             + credit04
             + credit05
             + credit06
             + credit07
             + credit08
             + credit09
             + credit10
             + credit11
             + credit12)
          - (  debit01
             + debit02
             + debit03
             + debit04
             + debit05
             + debit06
             + debit07
             + debit08
             + debit09
             + debit10
             + debit11
             + debit12)
             AS balance
   FROM   glg_unit_total;
END;

