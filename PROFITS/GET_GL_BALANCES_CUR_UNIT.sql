CREATE FUNCTION GET_GL_BALANCES_CUR_UNIT (
    IN_REPORT	VARCHAR(20),
    IN_EXCEL_SHEET	VARCHAR(20),
    IN_EXCEL_CELL	VARCHAR(20),
    IN_DATE	DATE,
    IN_CURRENCY	VARCHAR(10),
    IN_UNIT	VARCHAR(10) )
  RETURNS DECIMAL(30, 2)
  SPECIFIC SQL160729100041209
  NOT SECURED
  LANGUAGE SQL
  NOT DETERMINISTIC
  EXTERNAL ACTION
  READS SQL DATA
  INHERIT SPECIAL REGISTERS
BEGIN
DECLARE v_balance        DECIMAL (30, 2) DEFAULT 0;
DECLARE v_balance_from   DECIMAL (30, 2);
DECLARE v_balance_to     DECIMAL (30, 2);
  SELECT   SUM(t.factor1
  * (  mcb_report_pkg.get_gl_balance (
       account_id
      , in_currency
      , in_unit
      ,mcb_report_pkg.get_desired_gl_date ( in_date, t.date_to)
      ,mcb_report_pkg.get_desired_gl_date ( in_date, t.date_to)
      ,lc_indicator
      ,zb_indicator)
  -   (DECODE (t.amt_type, 'CUM_DEB_CRE_LC', 0, 1)
    * mcb_report_pkg.get_gl_balance (
         account_id
        ,in_currency
        ,in_unit
        ,mcb_report_pkg.get_desired_gl_date ( in_date, t.date_from)
        ,mcb_report_pkg.get_desired_gl_date ( in_date, t.date_from)
        ,lc_indicator
        ,zb_indicator)
        ))
        )
  INTO v_balance                
  FROM   (SELECT gl_sn
      ,gl_account_from
      ,a1.account_id
      ,gl_account_to
      ,amt_type
      ,currency_type
      ,currency
      ,factor1
      ,date_from
      ,date_to
      ,DECODE (
          amt_type
         ,'DEBIT', '2'
         ,'DEBIT_BD', '3'
         ,'DEBIT_ZB', '4'
         ,'CREDIT', '5'
         ,'CREDIT_BD', '6'
         ,'CREDIT_ZB', '7'
         ,'DEBIT_PLUS_BD', '8'
         ,'CREDIT_PLUS_BD', '9'
         ,'DEBIT_ALL', '10'
         ,'CREDIT_ALL', '11'
         ,0)
          AS zb_indicator
      ,DECODE (currency_type, 'LOCAL', 'LC', 'FC') AS lc_indicator
  FROM   profits_gl_configuration a, glg_account a1
  WHERE      a.report = in_report
       AND a.excel_sheet = in_excel_sheet
       AND a.excel_cell = in_excel_cell
       AND a1.subs_cons_flag = 1
       AND TRIM (a1.account_id) BETWEEN a.gl_account_from
                                    AND a.gl_account_to) t;
RETURN v_balance;
end;

