CREATE PROCEDURE update_w_agg_gl_unit_totals
BEGIN
	DECLARE  l_count DECIMAL (10);
	DECLARE  l_backdate_ind DECIMAL (1);
	DECLARE  l_zerobal_ind DECIMAL (1);
	DECLARE  l_transferbal_ind DECIMAL (1);
	DECLARE  l_debit DECIMAL (30, 2);
	DECLARE  l_credit DECIMAL (30, 2);
	DECLARE  l_debit_bd DECIMAL (30, 2);
	DECLARE  l_credit_bd DECIMAL (30, 2);
	DECLARE  l_debit_zb DECIMAL (30, 2);
	DECLARE  l_credit_zb DECIMAL (30, 2);
	DECLARE  l_debit_tb DECIMAL (30, 2);
	DECLARE  l_credit_tb DECIMAL (30, 2);
	DECLARE  l_prev_bal DECIMAL (30, 2);
	DECLARE  l_prev_bal_zb DECIMAL (30, 2);
	DECLARE  l_prev_daily_bal_zb DECIMAL (30, 2);
	DECLARE  l_prev_executed_date DATE;
	DECLARE  l_acc_kind DECIMAL (5);
	DECLARE  l_each_maxdate DATE;
	DECLARE  l_default_cost_id VARCHAR (10);
 
   SET l_default_cost_id =  '          ';
 
   SET l_prev_executed_date = DATE '1900-01-01';
 
   FOR entry AS (SELECT   a.gl_trn_date
                         ,a.fk_glg_accountacco
                         ,a.fk_currencyid_curr
                         ,a.fk1unitcode
                         ,a.trn_date
                         ,a.entry_type
                         ,a.amount
                         ,a.fk_glg_documentdoc
                         ,a.fk_glg_justifyjust
                         ,a.fk_glg_documentdo0
                         ,a.remarks
                         ,a.fk_unitcode
                         ,a.fk_usrcode
                         ,a.line_num
                         ,a.trn_snum
                 FROM     glg_final_trn a
                 WHERE    NVL (a.eom_update, '0') = '0'
                 ORDER BY 1, 2, 3, 4, 5, 6)
   DO
      IF entry.gl_trn_date <> l_prev_executed_date
      THEN
         FOR prev_agg
            AS (SELECT   b.fk_glg_accountacco
                        ,b.fk_currencyid_curr
                        ,b.fk_unitcode
                        ,b.gl_date
                        ,b.balance
                        ,b.balance_zb
                        ,b.debit_zb
                        ,b.credit_zb
                FROM     w_agg_gl_unit_totals b
                WHERE        b.fk_cost_id = l_default_cost_id
                         AND b.gl_date = (SELECT MAX (b2.gl_date)
                                          FROM   w_agg_gl_unit_totals b2
                                          WHERE  b2.gl_date < entry.gl_trn_date)
                         AND b.fk_company_code = 1000
                ORDER BY 1, 2, 3, 4)
         DO
            SELECT COUNT (*)
            INTO   l_count
            FROM   w_agg_gl_unit_totals b
            WHERE      b.fk_cost_id = l_default_cost_id
                   AND b.fk_currencyid_curr = prev_agg.fk_currencyid_curr
                   AND b.fk_unitcode = prev_agg.fk_unitcode
                   AND b.fk_glg_accountacco = prev_agg.fk_glg_accountacco
                   AND b.gl_date = entry.gl_trn_date
                   AND b.fk_company_code = 1000;
            IF l_count = 0
            THEN
               SET l_prev_daily_bal_zb = prev_agg.debit_zb - prev_agg.credit_zb;
               SET l_prev_bal = l_prev_daily_bal_zb + prev_agg.balance;
               SET l_prev_bal_zb = prev_agg.balance_zb;
               SELECT MAX (a.fkgd_has_a_primary)
               INTO   l_acc_kind
               FROM   glg_account b
                      LEFT JOIN par_relation_detai a
                         ON a.fkgd_has_a_seconda = b.bop_group_account
               WHERE      b.account_id = prev_agg.fk_glg_accountacco
                      AND a.fk_par_relationcod = 'BDMONKIN'
                      AND a.fkgd_has_a_primary IN (3, 4);
               IF l_acc_kind IS NOT NULL
               THEN
                  SET l_prev_bal = l_prev_bal_zb;
                  IF TO_NUMBER (EXTRACT (YEAR FROM prev_agg.gl_date)) <>
                        TO_NUMBER (EXTRACT (YEAR FROM entry.gl_trn_date))
                  THEN
                     SET l_prev_bal = 0;
                     SET l_prev_bal_zb = 0;
                  END IF;
               END IF;
               INSERT INTO w_agg_gl_unit_totals (
                              lst_updat_date
                             ,credit
                             ,debit
                             ,fk_company_code
                             ,gl_date
                             ,fk_cost_id
                             ,fk_currencyid_curr
                             ,fk_unitcode
                             ,fk_glg_accountacco
                             ,debit_zb
                             ,debit_bd
                             ,credit_zb
                             ,credit_bd
                             ,balance_zb
                             ,daily_balance_zb
                             ,balance
                             ,daily_balance
                             ,debit_tb
                             ,credit_tb)
                  SELECT bp.scheduled_date AS lst_updat_date
                        ,0 AS credit
                        ,0 AS debit
                        ,1000 AS fk_company_code
                        ,entry.gl_trn_date AS gl_date
                        ,l_default_cost_id AS fk_cost_id
                        ,prev_agg.fk_currencyid_curr AS fk_currencyid_curr
                        ,prev_agg.fk_unitcode AS fk_unitcode
                        ,prev_agg.fk_glg_accountacco AS fk_glg_accountacco
                        ,0 AS debit_zb
                        ,0 AS debit_bd
                        ,0 AS credit_zb
                        ,0 AS credit_bd
                        ,l_prev_bal AS l_prev_bal_zb
                        ,0 AS daily_balance_zb
                        ,prev_agg.balance AS balance
                        ,0 AS daily_balance
                        ,0 AS debit_tb
                        ,0 AS credit_tb
                  FROM   bank_parameters bp;
            END IF;
         END FOR;
         SET l_prev_executed_date = entry.gl_trn_date;
      END IF;
      SET l_debit = 0;
      SET l_credit = 0;
      SET l_debit_bd = 0;
      SET l_credit_bd = 0;
      SET l_debit_zb = 0;
      SET l_credit_zb = 0;
      SET l_debit_tb = 0;
      SET l_credit_tb = 0;
      SET l_zerobal_ind = 0;
      SET l_backdate_ind = 0;
      SET l_transferbal_ind = 0;
      IF entry.gl_trn_date <> entry.trn_date
      THEN
         SET l_backdate_ind = 1;
         SET l_zerobal_ind = 0;
         SET l_transferbal_ind = 0;
         IF (   (    entry.fk_glg_documentdoc = 'YCL2'
                 AND entry.fk_glg_justifyjust NOT IN ('OCNT')
                 AND entry.remarks = 'PREV YEAR ZERO BAL')
             OR (    entry.fk_glg_documentdoc = 'YCL2'
                 AND entry.fk_glg_justifyjust = 'OYCA'))
         THEN
            -- Zeroising Balances at EOY
            SET l_zerobal_ind = 1;
            SET l_transferbal_ind = 0;
         END IF;
         IF (    entry.fk_glg_documentdoc = 'INV2'
             AND entry.fk_glg_justifyjust = 'OYCZ'
             AND entry.fk_glg_documentdo0 = 'IN')
         THEN
            -- Transfer Balances to new year
            SET l_transferbal_ind = 1;
            SET l_zerobal_ind = 0;
         END IF;
      ELSE
         SET l_backdate_ind = 0;
      END IF;
      IF l_backdate_ind = 0
      THEN
         IF entry.entry_type = '1'
         THEN
            SET l_debit = entry.amount;
         ELSE
            IF entry.entry_type = '2'
            THEN
               SET l_credit = entry.amount;
            END IF;
         END IF;
      END IF;
      IF l_backdate_ind = 1
      THEN
         IF (l_zerobal_ind = 0 AND l_transferbal_ind = 0 AND entry.entry_type = '1')
         THEN
            SET l_debit_bd = entry.amount;
         END IF;
         IF (l_zerobal_ind = 0 AND l_transferbal_ind = 0 AND entry.entry_type = '2')
         THEN
            SET l_credit_bd = entry.amount;
         END IF;
         IF (l_zerobal_ind = 1 AND entry.entry_type = '1')
         THEN
            SET l_debit_zb = entry.amount;
         END IF;
         IF (l_zerobal_ind = 1 AND entry.entry_type = '2')
         THEN
            SET l_credit_zb = entry.amount;
         END IF;
         IF (l_transferbal_ind = 1 AND entry.entry_type = '1')
         THEN
            SET l_debit_tb = entry.amount;
         END IF;
         IF (l_transferbal_ind = 1 AND entry.entry_type = '2')
         THEN
            SET l_credit_tb = entry.amount;
         END IF;
      END IF;
      INSERT INTO w_agg_gl_unit_totals (
                     lst_updat_date
                    ,credit
                    ,debit
                    ,fk_company_code
                    ,gl_date
                    ,fk_cost_id
                    ,fk_currencyid_curr
                    ,fk_unitcode
                    ,fk_glg_accountacco
                    ,debit_zb
                    ,debit_bd
                    ,credit_zb
                    ,credit_bd
                    ,balance_zb
                    ,daily_balance_zb
                    ,balance
                    ,daily_balance
                    ,debit_tb
                    ,credit_tb)
         SELECT bp.scheduled_date AS lst_updat_date
               ,0 AS credit
               ,0 AS debit
               ,1000 AS fk_company_code
               ,entry.gl_trn_date AS gl_date
               ,l_default_cost_id AS fk_cost_id
               ,entry.fk_currencyid_curr AS fk_currencyid_curr
               ,entry.fk1unitcode AS fk_unitcode
               ,entry.fk_glg_accountacco AS fk_glg_accountacco
               ,0 AS debit_zb
               ,0 AS debit_bd
               ,0 AS credit_zb
               ,0 AS credit_bd
               ,0 AS balance_zb
               ,0 AS daily_balance_zb
               ,0 AS balance
               ,0 AS daily_balance
               ,0 AS debit_tb
               ,0 AS credit_tb
         FROM   bank_parameters bp
         WHERE  NOT EXISTS
                       (SELECT 'x'
                        FROM   w_agg_gl_unit_totals b
                        WHERE      b.fk_cost_id = l_default_cost_id
                               AND b.fk_currencyid_curr =
                                      entry.fk_currencyid_curr
                               AND b.fk_unitcode = entry.fk1unitcode
                               AND b.fk_glg_accountacco =
                                      entry.fk_glg_accountacco
                               AND b.gl_date = entry.gl_trn_date
                               AND b.fk_company_code = 1000);
      INSERT INTO w_agg_gl_unit_totals (
                     lst_updat_date
                    ,credit
                    ,debit
                    ,fk_company_code
                    ,gl_date
                    ,fk_cost_id
                    ,fk_currencyid_curr
                    ,fk_unitcode
                    ,fk_glg_accountacco
                    ,debit_zb
                    ,debit_bd
                    ,credit_zb
                    ,credit_bd
                    ,balance_zb
                    ,daily_balance_zb
                    ,balance
                    ,daily_balance
                    ,debit_tb
                    ,credit_tb)
         SELECT   bp.scheduled_date AS lst_updat_date
                 ,0 AS credit
                 ,0 AS debit
                 ,1000 AS fk_company_code
                 ,later_date.gl_date AS gl_date
                 ,l_default_cost_id AS fk_cost_id
                 ,entry.fk_currencyid_curr AS fk_currencyid_curr
                 ,entry.fk1unitcode AS fk_unitcode
                 ,entry.fk_glg_accountacco AS fk_glg_accountacco
                 ,0 AS debit_zb
                 ,0 AS debit_bd
                 ,0 AS credit_zb
                 ,0 AS credit_bd
                 ,0 AS balance_zb
                 ,0 AS daily_balance_zb
                 ,0 AS balance
                 ,0 AS daily_balance
                 ,0 AS debit_tb
                 ,0 AS credit_tb
         FROM     bank_parameters bp, w_agg_gl_unit_totals later_date
         WHERE        later_date.gl_date >= entry.gl_trn_date
                  AND NOT EXISTS
                             (SELECT 'x'
                              FROM   w_agg_gl_unit_totals b
                              WHERE      b.fk_cost_id = l_default_cost_id
                                     AND b.fk_currencyid_curr =
                                            entry.fk_currencyid_curr
                                     AND b.fk_unitcode = entry.fk1unitcode
                                     AND b.fk_glg_accountacco =
                                            entry.fk_glg_accountacco
                                     AND b.gl_date = later_date.gl_date
                                     AND b.fk_company_code = 1000)
         GROUP BY later_date.gl_date, bp.scheduled_date;
      UPDATE w_agg_gl_unit_totals b
      SET    debit = debit + NVL (l_debit, 0)
            ,credit = credit + NVL (l_credit, 0)
            ,debit_bd = debit_bd + NVL (l_debit_bd, 0)
            ,credit_bd = credit_bd + NVL (l_credit_bd, 0)
            ,debit_zb = debit_zb + NVL (l_debit_zb, 0)
            ,credit_zb = credit_zb + NVL (l_credit_zb, 0)
            ,debit_tb = debit_tb + NVL (l_debit_tb, 0)
            ,credit_tb = credit_tb + NVL (l_credit_tb, 0)
      WHERE      b.fk_cost_id = l_default_cost_id
             AND b.fk_currencyid_curr = entry.fk_currencyid_curr
             AND b.fk_unitcode = entry.fk1unitcode
             AND b.fk_glg_accountacco = entry.fk_glg_accountacco
             AND b.gl_date = entry.gl_trn_date
             AND b.fk_company_code = 1000;
      UPDATE w_agg_gl_unit_totals b
      SET    daily_balance = debit + debit_bd - (credit + credit_bd)
            ,daily_balance_zb =
                  debit
                + debit_bd
                + debit_zb
                + debit_tb
                - (credit + credit_bd + credit_zb + credit_tb)
      WHERE      b.fk_cost_id = l_default_cost_id
             AND b.fk_currencyid_curr = entry.fk_currencyid_curr
             AND b.fk_unitcode = entry.fk1unitcode
             AND b.fk_glg_accountacco = entry.fk_glg_accountacco
             AND b.gl_date = entry.gl_trn_date
             AND b.fk_company_code = 1000;
      -- update glg_final_trn that the record has been processed
      UPDATE glg_final_trn a
      SET    eom_update = '1'
      WHERE      a.trn_date = entry.trn_date
             AND a.fk_unitcode = entry.fk_unitcode
             AND a.fk_usrcode = entry.fk_usrcode
             AND a.line_num = entry.line_num
             AND a.trn_snum = entry.trn_snum;
      -- update progressive balances for all the relative records will equal or later date
      FOR agg
         AS (SELECT   b.fk_glg_accountacco
                     ,b.fk_currencyid_curr
                     ,b.fk_unitcode
                     ,b.gl_date
             FROM     w_agg_gl_unit_totals b
             WHERE        b.fk_cost_id = l_default_cost_id
                      AND b.fk_currencyid_curr = entry.fk_currencyid_curr
                      AND b.fk_unitcode = entry.fk1unitcode
                      AND b.fk_glg_accountacco = entry.fk_glg_accountacco
                      AND b.gl_date >= entry.gl_trn_date
                      AND b.fk_company_code = 1000
             ORDER BY 1, 2, 3, 4)
      DO
         SELECT NVL (MAX (b.balance), 0)
               ,NVL (MAX (b.balance_zb), 0)
               ,NVL (MAX (b.debit_zb - b.credit_zb), 0)
         INTO   l_prev_bal, l_prev_bal_zb, l_prev_daily_bal_zb
         FROM   w_agg_gl_unit_totals b
         WHERE      b.fk_cost_id = l_default_cost_id
                AND b.fk_currencyid_curr = agg.fk_currencyid_curr
                AND b.fk_unitcode = agg.fk_unitcode
                AND b.fk_glg_accountacco = agg.fk_glg_accountacco
                AND b.gl_date =
                       (SELECT MAX (b2.gl_date)
                        FROM   w_agg_gl_unit_totals b2
                        WHERE      b2.fk_cost_id = b.fk_cost_id
                               AND b2.fk_currencyid_curr = b.fk_currencyid_curr
                               AND b2.fk_unitcode = b.fk_unitcode
                               AND b2.fk_glg_accountacco = b.fk_glg_accountacco
                               AND b2.gl_date < agg.gl_date
                               AND b2.fk_company_code = b.fk_company_code)
                AND b.fk_company_code = 1000;
         SET l_prev_bal = l_prev_daily_bal_zb + l_prev_bal;
         SELECT MAX (b.gl_date)
         INTO   l_each_maxdate
         FROM   w_agg_gl_unit_totals b
         WHERE      b.fk_cost_id = l_default_cost_id
                AND b.fk_currencyid_curr = agg.fk_currencyid_curr
                AND b.fk_unitcode = agg.fk_unitcode
                AND b.fk_glg_accountacco = agg.fk_glg_accountacco
                AND b.gl_date < agg.gl_date
                AND b.fk_company_code = 1000;
         SELECT MAX (a.fkgd_has_a_primary)
         INTO   l_acc_kind
         FROM   glg_account b
                LEFT JOIN par_relation_detai a
                   ON a.fkgd_has_a_seconda = b.bop_group_account
         WHERE      b.account_id = agg.fk_glg_accountacco
                AND a.fk_par_relationcod = 'BDMONKIN'
                AND a.fkgd_has_a_primary IN (3, 4);
         IF l_acc_kind IS NULL
         THEN
            SET l_prev_bal = l_prev_bal_zb;
            IF TO_NUMBER (EXTRACT (YEAR FROM agg.gl_date)) <>
                  TO_NUMBER (EXTRACT (YEAR FROM l_each_maxdate))
            THEN
               SET l_prev_bal = 0;
               SET l_prev_bal_zb = 0;
            END IF;
         END IF;
         UPDATE w_agg_gl_unit_totals b
         SET    balance = daily_balance + l_prev_bal + debit_tb - credit_tb
               ,balance_zb = daily_balance_zb + l_prev_bal_zb
         WHERE      b.fk_cost_id = l_default_cost_id
                AND b.fk_currencyid_curr = agg.fk_currencyid_curr
                AND b.fk_unitcode = agg.fk_unitcode
                AND b.fk_glg_accountacco = agg.fk_glg_accountacco
                AND b.gl_date = agg.gl_date
                AND b.fk_company_code = 1000;
      END FOR;
   END FOR;
END;

