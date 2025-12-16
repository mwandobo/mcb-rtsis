create table EOM_GLG_ENTEP_TOTAL
(
    EOM_DATE           DATE     not null,
    FK_GLG_ACCOUNTACCO CHAR(21) not null,
    FK_CURRENCYID_CURR INTEGER  not null,
    YEAR0              SMALLINT not null,
    INVENT_CR          DECIMAL(15, 2),
    INVENT_DB          DECIMAL(15, 2),
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
    MAX_BAL_1          DECIMAL(15, 2),
    MAX_BAL_2          DECIMAL(15, 2),
    MAX_BAL_3          DECIMAL(15, 2),
    MAX_BAL_4          DECIMAL(15, 2),
    MAX_BAL_5          DECIMAL(15, 2),
    MAX_BAL_6          DECIMAL(15, 2),
    MAX_BAL_7          DECIMAL(15, 2),
    MAX_BAL_8          DECIMAL(15, 2),
    MAX_BAL_9          DECIMAL(15, 2),
    MAX_BAL_10         DECIMAL(15, 2),
    MAX_BAL_11         DECIMAL(15, 2),
    MAX_BAL_12         DECIMAL(15, 2),
    MIN_BAL_1          DECIMAL(15, 2),
    MIN_BAL_2          DECIMAL(15, 2),
    MIN_BAL_3          DECIMAL(15, 2),
    MIN_BAL_4          DECIMAL(15, 2),
    MIN_BAL_5          DECIMAL(15, 2),
    MIN_BAL_6          DECIMAL(15, 2),
    MIN_BAL_7          DECIMAL(15, 2),
    MIN_BAL_8          DECIMAL(15, 2),
    MIN_BAL_9          DECIMAL(15, 2),
    MIN_BAL_10         DECIMAL(15, 2),
    MIN_BAL_11         DECIMAL(15, 2),
    MIN_BAL_12         DECIMAL(15, 2),
    BAL_SHEET_CR       DECIMAL(15, 2),
    BAL_SHEET_DB       DECIMAL(15, 2),
    MAX_BAL_SHEET      DECIMAL(15, 2),
    MIN_BAL_SHEET      DECIMAL(15, 2),
    AVER_BAL_SHEET     DECIMAL(15, 2),
    YR_AVR_BAL         DECIMAL(15, 2),
    LST_UPDAT_DATE     DATE,
    LEVEL0             CHAR(1),
    constraint IXU_EOM_005
        primary key (EOM_DATE, FK_GLG_ACCOUNTACCO, FK_CURRENCYID_CURR, YEAR0)
);

CREATE PROCEDURE EOM_GLG_ENTEP_TOTAL ( )
  SPECIFIC SQL160620112634363
  LANGUAGE SQL
  NOT DETERMINISTIC
  EXTERNAL ACTION
  MODIFIES SQL DATA
  CALLED ON NULL INPUT
  INHERIT SPECIAL REGISTERS
  OLD SAVEPOINT LEVEL
BEGIN
DELETE eom_glg_entep_total
WHERE  eom_date = (SELECT scheduled_date FROM bank_parameters);
INSERT INTO eom_glg_entep_total (
               fk_glg_accountacco
              ,fk_currencyid_curr
              ,year0
              ,invent_cr
              ,invent_db
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
              ,max_bal_1
              ,max_bal_2
              ,max_bal_3
              ,max_bal_4
              ,max_bal_5
              ,max_bal_6
              ,max_bal_7
              ,max_bal_8
              ,max_bal_9
              ,max_bal_10
              ,max_bal_11
              ,max_bal_12
              ,min_bal_1
              ,min_bal_2
              ,min_bal_3
              ,min_bal_4
              ,min_bal_5
              ,min_bal_6
              ,min_bal_7
              ,min_bal_8
              ,min_bal_9
              ,min_bal_10
              ,min_bal_11
              ,min_bal_12
              ,bal_sheet_cr
              ,bal_sheet_db
              ,max_bal_sheet
              ,min_bal_sheet
              ,aver_bal_sheet
              ,level0
              ,yr_avr_bal
              ,lst_updat_date
              ,eom_date)
   SELECT fk_glg_accountacco
         ,fk_currencyid_curr
         ,year0
         ,invent_cr
         ,invent_db
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
         ,max_bal_1
         ,max_bal_2
         ,max_bal_3
         ,max_bal_4
         ,max_bal_5
         ,max_bal_6
         ,max_bal_7
         ,max_bal_8
         ,max_bal_9
         ,max_bal_10
         ,max_bal_11
         ,max_bal_12
         ,min_bal_1
         ,min_bal_2
         ,min_bal_3
         ,min_bal_4
         ,min_bal_5
         ,min_bal_6
         ,min_bal_7
         ,min_bal_8
         ,min_bal_9
         ,min_bal_10
         ,min_bal_11
         ,min_bal_12
         ,bal_sheet_cr
         ,bal_sheet_db
         ,max_bal_sheet
         ,min_bal_sheet
         ,aver_bal_sheet
         ,level0
         ,yr_avr_bal
         ,lst_updat_date
         , (SELECT scheduled_date FROM bank_parameters) AS eom_date
   FROM   glg_entep_total;
END;

