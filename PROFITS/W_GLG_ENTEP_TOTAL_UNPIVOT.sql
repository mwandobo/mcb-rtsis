CREATE VIEW W_GLG_ENTEP_TOTAL_UNPIVOT
(
   YEAR0,
   CURRENCY_ID,
   GL_ACCOUNT,
   UNIT_CODE,
   LEVEL0,
   MONTH_SEQ_NO,
   CURRENCY_CODE,
   DEBIT_AMOUNT,
   CREDIT_AMOUNT
)
AS
   SELECT glg_unit_total.year0,
          glg_unit_total.fk_currencyid_curr currency_id,
          glg_unit_total.fk_glg_accountacco gl_account,
          glg_unit_total.fk_unitcode unit_code,
          level0,
          month_seq_no,
          short_descr currency_code,
            flag_01 * debit01
          + flag_02 * (debit01 + debit02)
          + flag_03 * (debit01 + debit02 + debit03)
          + flag_04 * (debit01 + debit02 + debit03 + debit04)
          + flag_05 * (debit01 + debit02 + debit03 + debit04 + debit05)
          +   flag_06
            * (debit01 + debit02 + debit03 + debit04 + debit05 + debit06)
          +   flag_07
            * (  debit01
               + debit02
               + debit03
               + debit04
               + debit05
               + debit06
               + debit07)
          +   flag_08
            * (  debit01
               + debit02
               + debit03
               + debit04
               + debit05
               + debit06
               + debit07
               + debit08)
          +   flag_09
            * (  debit01
               + debit02
               + debit03
               + debit04
               + debit05
               + debit06
               + debit07
               + debit08
               + debit09)
          +   flag_10
            * (  debit01
               + debit02
               + debit03
               + debit04
               + debit05
               + debit06
               + debit07
               + debit08
               + debit09
               + debit10)
          +   flag_11
            * (  debit01
               + debit02
               + debit03
               + debit04
               + debit05
               + debit06
               + debit07
               + debit08
               + debit09
               + debit10
               + debit11)
          +   flag_12
            * (  debit01
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
          +   flag_13
            * (  debit01
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
               + debit12
               + bal_sheet_db)
             debit_amount,
            flag_01 * credit01
          + flag_02 * (credit01 + credit02)
          + flag_03 * (credit01 + credit02 + credit03)
          + flag_04 * (credit01 + credit02 + credit03 + credit04)
          + flag_05 * (credit01 + credit02 + credit03 + credit04 + credit05)
          +   flag_06
            * (  credit01
               + credit02
               + credit03
               + credit04
               + credit05
               + credit06)
          +   flag_07
            * (  credit01
               + credit02
               + credit03
               + credit04
               + credit05
               + credit06
               + credit07)
          +   flag_08
            * (  credit01
               + credit02
               + credit03
               + credit04
               + credit05
               + credit06
               + credit07
               + credit08)
          +   flag_09
            * (  credit01
               + credit02
               + credit03
               + credit04
               + credit05
               + credit06
               + credit07
               + credit08
               + credit09)
          +   flag_10
            * (  credit01
               + credit02
               + credit03
               + credit04
               + credit05
               + credit06
               + credit07
               + credit08
               + credit09
               + credit10)
          +   flag_11
            * (  credit01
               + credit02
               + credit03
               + credit04
               + credit05
               + credit06
               + credit07
               + credit08
               + credit09
               + credit10
               + credit11)
          +   flag_12
            * (  credit01
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
          +   flag_13
            * (  credit01
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
               + credit12
               + bal_sheet_cr)
             credit_amount
     FROM glg_unit_total
          JOIN currency ON glg_unit_total.fk_currencyid_curr = id_currency
          JOIN month_seq ON (1 = 1)
          JOIN glg_entep_ctl ON (1 = 1);

