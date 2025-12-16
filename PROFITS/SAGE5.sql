CREATE VIEW sage5 as

select glg.ACCOUNT_ID,curr.SHORT_DESCR,u.CODE,EOM_DATE,
(select (EOMp.DEBIT01+ EOMp.DEBIT02+ EOMp.DEBIT03+ EOMp.DEBIT04+ EOMp.DEBIT05+ EOMp.DEBIT06+ EOMp.DEBIT07+ EOMp.DEBIT08+
EOMp.DEBIT09+ EOMp.DEBIT10+ EOMp.DEBIT11+ EOMp.DEBIT12+ EOMp.BAL_SHEET_DB)-
(EOMp.CREDIT01+EOMp.CREDIT02+ EOMp.CREDIT03+ EOMp.CREDIT04+ EOMp.CREDIT05+ EOMp.CREDIT06+ EOMp.CREDIT07+ EOMp.CREDIT08+ 
EOMp.CREDIT09+ EOMp.CREDIT10+ EOMp.CREDIT11+ EOMp.CREDIT12+ EOMp.BAL_SHEET_CR) AS EOM_BALANCE_bf
from EOM_GLG_UNIT_TOTAL eomp
inner join CURRENCY curr on (curr.ID_CURRENCY = eomp.FK_CURRENCYID_CURR)
inner join unit u on (u.CODE = eomp.FK_UNITCODE)
inner join GLG_ACCOUNT glg on (glg.ACCOUNT_ID = eomp.FK_GLG_ACCOUNTACCO)
where eomp.EOM_DATE  = to_date('29-3-2025','dd-mm-yyyy')
and eomp.year0 in (2024, 2025)
and eomp.LEVEL0=5
AND eomp.YEAR0=eom.YEAR0
AND eomp.FK_CURRENCYID_CURR=eom.FK_CURRENCYID_CURR
AND eomp.FK_GLG_ACCOUNTACCO=eom.FK_GLG_ACCOUNTACCO
AND eomp.FK_UNITCODE=eom.FK_UNITCODE),
(EOM.DEBIT01+ EOM.DEBIT02+ EOM.DEBIT03+ EOM.DEBIT04+ EOM.DEBIT05+ EOM.DEBIT06+ EOM.DEBIT07+ EOM.DEBIT08+
EOM.DEBIT09+ EOM.DEBIT10+ EOM.DEBIT11+ EOM.DEBIT12+ EOM.BAL_SHEET_DB)-
(EOM.CREDIT01+EOM.CREDIT02+ EOM.CREDIT03+ EOM.CREDIT04+ EOM.CREDIT05+ EOM.CREDIT06+ EOM.CREDIT07+ EOM.CREDIT08+ 
EOM.CREDIT09+ EOM.CREDIT10+ EOM.CREDIT11+ EOM.CREDIT12+ EOM.BAL_SHEET_CR) AS EOM_BALANCE,
--
(SELECT rate
  FROM fixing_rate
WHERE (fk_currencyid_curr, activation_date, activation_time) IN (  SELECT fk_currencyid_curr,
                                                                           activation_date,
                                                                           MAX (
                                                                              activation_time)
                                                                              activation_time
                                                                     FROM fixing_rate
                                                                     where activation_date =(select max(b.activation_date) from fixing_rate b
where b.ACTIVATION_DATE<= to_date('02-4-2025','dd-mm-yyyy') and b.fk_currencyid_curr=curr.ID_CURRENCY)
                                                                  GROUP BY fk_currencyid_curr,
                                                                           activation_date)
and fk_currencyid_curr=curr.ID_CURRENCY) as rate,
round(((EOM.DEBIT01+ EOM.DEBIT02+ EOM.DEBIT03+ EOM.DEBIT04+ EOM.DEBIT05+ EOM.DEBIT06+ EOM.DEBIT07+ EOM.DEBIT08+
EOM.DEBIT09+ EOM.DEBIT10+ EOM.DEBIT11+ EOM.DEBIT12+ EOM.BAL_SHEET_DB)-
(EOM.CREDIT01+EOM.CREDIT02+ EOM.CREDIT03+ EOM.CREDIT04+ EOM.CREDIT05+ EOM.CREDIT06+ EOM.CREDIT07+ EOM.CREDIT08+ 
EOM.CREDIT09+ EOM.CREDIT10+ EOM.CREDIT11+ EOM.CREDIT12+ EOM.BAL_SHEET_CR))*(SELECT rate
  FROM fixing_rate
WHERE (fk_currencyid_curr, activation_date, activation_time) IN (  SELECT fk_currencyid_curr,
                                                                           activation_date,
                                                                           MAX (
                                                                              activation_time)
                                                                              activation_time
                                                                      FROM fixing_rate
                                                                     where activation_date =(select max(b.activation_date) from fixing_rate b
where b.ACTIVATION_DATE<= to_date('02-4-2025','dd-mm-yyyy') and b.fk_currencyid_curr=curr.ID_CURRENCY)
                                                                  GROUP BY fk_currencyid_curr,
                                                                           activation_date)
and fk_currencyid_curr=curr.ID_CURRENCY),2) as eom_balance_lc,
(EOM.DEBIT01+ EOM.DEBIT02+ EOM.DEBIT03+ EOM.DEBIT04+ EOM.DEBIT05+ EOM.DEBIT06+ EOM.DEBIT07+ EOM.DEBIT08+
EOM.DEBIT09+ EOM.DEBIT10+ EOM.DEBIT11+ EOM.DEBIT12+ EOM.BAL_SHEET_DB)-
(EOM.CREDIT01+EOM.CREDIT02+ EOM.CREDIT03+ EOM.CREDIT04+ EOM.CREDIT05+ EOM.CREDIT06+ EOM.CREDIT07+ EOM.CREDIT08+ 
EOM.CREDIT09+ EOM.CREDIT10+ EOM.CREDIT11+ EOM.CREDIT12+ EOM.BAL_SHEET_CR)-(select (EOMp.DEBIT01+ EOMp.DEBIT02+ EOMp.DEBIT03+ EOMp.DEBIT04+ EOMp.DEBIT05+ EOMp.DEBIT06+ EOMp.DEBIT07+ EOMp.DEBIT08+
EOMp.DEBIT09+ EOMp.DEBIT10+ EOMp.DEBIT11+ EOMp.DEBIT12+ EOMp.BAL_SHEET_DB)-
(EOMp.CREDIT01+EOMp.CREDIT02+ EOMp.CREDIT03+ EOMp.CREDIT04+ EOMp.CREDIT05+ EOMp.CREDIT06+ EOMp.CREDIT07+ EOMp.CREDIT08+ 
EOMp.CREDIT09+ EOMp.CREDIT10+ EOMp.CREDIT11+ EOMp.CREDIT12+ EOMp.BAL_SHEET_CR) AS EOM_BALANCE_bf
from EOM_GLG_UNIT_TOTAL eomp
inner join CURRENCY curr on (curr.ID_CURRENCY = eomp.FK_CURRENCYID_CURR)
inner join unit u on (u.CODE = eomp.FK_UNITCODE)
inner join GLG_ACCOUNT glg on (glg.ACCOUNT_ID = eomp.FK_GLG_ACCOUNTACCO)
where eomp.EOM_DATE = to_date('29-3-2025','dd-mm-yyyy')
and eomp.year0 in (2024, 2025)
and eomp.LEVEL0=5
AND eomp.YEAR0=eom.YEAR0
AND eomp.FK_CURRENCYID_CURR=eom.FK_CURRENCYID_CURR
AND eomp.FK_GLG_ACCOUNTACCO=eom.FK_GLG_ACCOUNTACCO
AND eomp.FK_UNITCODE=eom.FK_UNITCODE) as diff
from EOM_GLG_UNIT_TOTAL eom
inner join CURRENCY curr on (curr.ID_CURRENCY = eom.FK_CURRENCYID_CURR)
inner join unit u on (u.CODE = eom.FK_UNITCODE)
inner join GLG_ACCOUNT glg on (glg.ACCOUNT_ID = eom.FK_GLG_ACCOUNTACCO)
where eom.EOM_DATE =  to_date('02-4-2025','dd-mm-yyyy')
and eom.year0 in (2024, 2025)
and eom.LEVEL0=5
AND (
(EOM.DEBIT01+ EOM.DEBIT02+ EOM.DEBIT03+ EOM.DEBIT04+ EOM.DEBIT05+ EOM.DEBIT06+ EOM.DEBIT07+ EOM.DEBIT08+
EOM.DEBIT09+ EOM.DEBIT10+ EOM.DEBIT11+ EOM.DEBIT12+ EOM.BAL_SHEET_DB)-
(EOM.CREDIT01+EOM.CREDIT02+ EOM.CREDIT03+ EOM.CREDIT04+ EOM.CREDIT05+ EOM.CREDIT06+ EOM.CREDIT07+ EOM.CREDIT08+ 
EOM.CREDIT09+ EOM.CREDIT10+ EOM.CREDIT11+ EOM.CREDIT12+ EOM.BAL_SHEET_CR) <>0
OR
(select (EOMp.DEBIT01+ EOMp.DEBIT02+ EOMp.DEBIT03+ EOMp.DEBIT04+ EOMp.DEBIT05+ EOMp.DEBIT06+ EOMp.DEBIT07+ EOMp.DEBIT08+
EOMp.DEBIT09+ EOMp.DEBIT10+ EOMp.DEBIT11+ EOMp.DEBIT12+ EOMp.BAL_SHEET_DB)-
(EOMp.CREDIT01+EOMp.CREDIT02+ EOMp.CREDIT03+ EOMp.CREDIT04+ EOMp.CREDIT05+ EOMp.CREDIT06+ EOMp.CREDIT07+ EOMp.CREDIT08+ 
EOMp.CREDIT09+ EOMp.CREDIT10+ EOMp.CREDIT11+ EOMp.CREDIT12+ EOMp.BAL_SHEET_CR) AS EOM_BALANCE_bf
from EOM_GLG_UNIT_TOTAL eomp
inner join CURRENCY curr on (curr.ID_CURRENCY = eomp.FK_CURRENCYID_CURR)
inner join unit u on (u.CODE = eomp.FK_UNITCODE)
inner join GLG_ACCOUNT glg on (glg.ACCOUNT_ID = eomp.FK_GLG_ACCOUNTACCO)
where eomp.EOM_DATE = to_date('29-3-2025','dd-mm-yyyy')
and eomp.year0 in (2024, 2025)
and eomp.LEVEL0=5
AND eomp.YEAR0=eom.YEAR0
AND eomp.FK_CURRENCYID_CURR=eom.FK_CURRENCYID_CURR
AND eomp.FK_GLG_ACCOUNTACCO=eom.FK_GLG_ACCOUNTACCO
AND eomp.FK_UNITCODE=eom.FK_UNITCODE)<>0);

