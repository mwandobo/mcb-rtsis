CREATE VIEW W_FACT_LOAN_ACCT_LEDGER
AS
SELECT
n.account_ser_num acct_key,
e.acc_unit,
e.acc_type,
e.acc_sn,
e.tmstamp,
e.trx_internal_sn,
r.request_type,
DECODE (r.request_type,
'1', 'Instalment',
'2', 'Claimed Interest',
'3', 'Loan',
'4', 'Charges',
'5', 'Promissory Note',
'n/a') request_type_name ,
r.request_loan_sts,
DECODE (r.request_loan_sts,
'1', 'Normal',
'2', 'Overdue',
'3', 'Definite Delay',
'4', 'Write-off')request_loan_sts_name,
r.request_sn,
TRIM (n.account_number) || '-' || account_cd account_no,
e.trx_date,
e.trx_unit          ,
trx_usr trx_user          ,
rl_pnl_int_amn          ,
rl_nrm_int_amn          ,
commission_amn          ,
expense_amn          ,
capital_amn          ,
e.extrait_comments comments          ,
valeur_dt value_date          ,
trx_sn          ,
DECODE (e.reversed_flg,
 '1', 'Reversed', 'Posted')
 reversed_flag          ,
 transaction_code          ,
 CASE                WHEN
 e.transaction_code IN (74181 ,74191 ,74201,4261,4251,4241,74131,74111,74127)
 THEN                   0                ELSE                   1
  END           * (  CASE WHEN rl_pnl_int_amn > 0 THEN rl_pnl_int_amn ELSE 0
  END              + CASE WHEN rl_nrm_int_amn > 0 THEN rl_nrm_int_amn ELSE 0
  END              + CASE WHEN commission_amn > 0 THEN commission_amn ELSE 0
  END              + CASE WHEN expense_amn > 0 THEN expense_amn ELSE 0
  END              + CASE WHEN capital_amn > 0 THEN capital_amn ELSE 0
  END)              payment_amt,
  H.SERIAL_NUM as transaction_type_sn,
  H.DESCRIPTION as transaction_type
  FROM   loan_request r
  JOIN loan_account_extra e ON (e.acc_sn = r.fk_loan_accountacc
								AND e.acc_type = r.fk0loan_accountacc
								AND e.acc_unit = r.fk_loan_accountfk
								AND e.request_loan_sts = r.request_loan_sts
								AND e.request_sn = r.request_sn
								AND e.request_type = r.request_type)
  LEFT JOIN profits_account n ON (n.lns_sn = e.acc_sn
								  AND n.lns_type = e.acc_type
								  AND n.lns_open_unit = e.acc_unit)
  LEFT JOIN	GENERIC_DETAIL h ON (e.GD_REASON=h.SERIAL_NUM
								 AND e.GH_REASON=h.PARAMETER_TYPE)
WITH NO ROW MOVEMENT;

