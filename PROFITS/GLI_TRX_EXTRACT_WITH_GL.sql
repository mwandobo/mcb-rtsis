CREATE VIEW PROFITS.GLI_TRX_EXTRACT_WITH_GL
AS
SELECT a.gl_trn_date AS trx_gl_trn_date, a.trn_snum AS trx_sn, a.fk1unitcode, TRIM(a.fk_glg_accountacco) AS accountacco,
a.entry_type, CASE WHEN a.ENTRY_TYPE = '2' THEN -1 ELSE 1 END * a.amount AS amount,
C.short_desCR AS currency_short_des, TRIM(a.REMARKS) as REMARKS, '0' AS id_product,
to_number(decode(a.subsystem,'  ','00',a.subsystem)) AS subsystem, '0' as trx_code,
'0' as id_justific, NULL as justific_descr,
REPLACE(a.fk_glg_accountacco,'.','') as external_glaccount,
A.FK_CUSTOMERCODE AS cust_id,  '0' as gl_rule,
'0' as glacc_origin, '1' as amount_ser_no,
a.trn_date, a.fk_unitcode AS fk_unitcodetrxunit,
a.fk_usrcode,  a.fk_usrcode as trx_usr,
NULL AS prf_account_number, '0' AS account_number, '0' AS prf_acc_cd
FROM GLG_FINAL_TRN A LEFT JOIN CURRENCY C ON  C.ID_CURRENCY=A.FK_CURRENCYID_CURR
WHERE (a.SUBSYSTEM IN ('5','05'))
UNION
select
B.trx_gl_trn_date, B.trx_sn, B.fk1unitcode, TRIM(B.fk_glg_accountacco) AS accountacco,
B.entry_type, CASE WHEN B.ENTRY_TYPE = '2' THEN -1 ELSE 1 END * B.fc_amount AS amount,
B.currency_short_des, TRIM(B.entry_comments) as REMARKS, B.id_product,
to_number(decode(subsystem,'  ','00',subsystem)) AS subsystem, B.trx_code,
B.id_justific,
B.justific_descr, B.external_glaccount,
B.cust_id, B.gl_rule, B.glacc_origin,
B.amount_ser_no,
B.trn_date, B.fk_unitcodetrxunit, B.fk_usrcode, B.trx_usr,
B.prf_account_number, B.account_number, B.prf_acc_cd
FROM gli_trx_extract B
WITH NO ROW MOVEMENT;

