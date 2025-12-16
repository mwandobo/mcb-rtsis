CREATE PROCEDURE PROFITS.REFRESH_R_LOAN_TABLES_NEW ( )
BEGIN
   DELETE tmp_refresh_lns_inst_1;
   INSERT INTO tmp_refresh_lns_inst_1 (
                  acc_unit
                 ,acc_type
                 ,acc_sn
                 ,rl_pnl_int_amn
                 ,url_pnl_int_amn
                 ,acr_pnl_int_amn
                 ,tot_exp_in_cc_amn
                 ,tot_com_in_cc_amn
                 ,thrdprt_amn
                 ,tot_subs_int_amn
                 ,tot_expense_amn
                 ,tot_confirm_amn
                 ,positive_amn
                 ,unclear_amn
                 ,blocked_amn
                 ,dormant_amn
                 ,last_nrm_trx_cnt
                 ,request_sn
                 ,install_round_amn
                 ,counter
                 ,max_req_loan_status)
      SELECT   a.acc_unit
              ,a.acc_type
              ,a.acc_sn
              ,NVL( SUM (a.rl_pnl_int_amn), 0)
              ,NVL( SUM (a.url_pnl_int_amn), 0)
              ,NVL( SUM (a.acr_pnl_int_amn), 0)
              ,NVL( SUM (a.tot_exp_in_cc_amn), 0)
              ,NVL( SUM (a.tot_com_in_cc_amn), 0)
              ,NVL( SUM (a.thrdprt_amn), 0)
              ,NVL( SUM (a.tot_subs_int_amn), 0)
              ,NVL( SUM (a.tot_expense_amn), 0)
              ,NVL( SUM (a.tot_confirm_amn), 0)
              ,NVL( SUM (a.positive_amn), 0)
              ,NVL( SUM (a.unclear_amn), 0)
              ,NVL( SUM (a.blocked_amn), 0)
              ,NVL( SUM (a.dormant_amn), 0)
              ,NVL (MAX (a.last_nrm_trx_cnt), 0)
              ,NVL (MAX (a.request_sn), 0)
              , (SELECT NVL( SUM (c.install_round_amn), 0)
                 FROM   loan_trx_recording c
                 WHERE      c.trx_date = c.scheduled_date
                        AND c.tmstamp > b.lst_tmstamp
                        AND c.trx_internal_sn = 1
                        AND c.trx_code IN (74131, 74141, 74151)
                        AND c.acc_unit = a.acc_unit
                        AND c.acc_type = a.acc_type
                        AND c.acc_sn = a.acc_sn)
              , (SELECT COUNT (*)
                 FROM   loan_trx_recording
                 WHERE      trx_date = c.scheduled_date
                        AND acc_unit = a.acc_unit
                        AND acc_type = a.acc_type
                        AND acc_sn = a.acc_sn
                        AND tmstamp > b.lst_tmstamp
                        AND (   (trx_code = 74181 AND i_justification = '74182')
                             OR (trx_code = 74191 AND i_justification = '74192')
                             OR (trx_code = 74201 AND i_justification = '74202')
                             OR (trx_code = 74211 AND i_justification = '74212')))
              , (SELECT MAX (request_loan_sts)
                 FROM   loan_trx_recording
                 WHERE      trx_date = c.scheduled_date
                        AND acc_unit = a.acc_unit
                        AND acc_type = a.acc_type
                        AND acc_sn = a.acc_sn
                        AND tmstamp > b.lst_tmstamp
                        AND (   (trx_code = 74181 AND i_justification = '74182')
                             OR (trx_code = 74191 AND i_justification = '74192')
                             OR (trx_code = 74201 AND i_justification = '74202')
                             OR (trx_code = 74211 AND i_justification = '74212')))
      FROM     loan_trx_recording a, r_loan_lst_tmstamp b, bank_parameters c
      WHERE    a.trx_date = c.scheduled_date AND a.tmstamp > b.lst_tmstamp
      GROUP BY a.acc_unit
              ,a.acc_type
              ,a.acc_sn
              ,c.scheduled_date
              ,b.lst_tmstamp;
   DELETE tmp_refresh_lns_inst;
   INSERT INTO tmp_refresh_lns_inst (
                  acc_unit
                 ,acc_type
                 ,acc_sn
                 ,tot_pub_comm_amn
                 ,tot_int_sprd_amn
                 ,tot_thrdprt_amn
                 ,mp_start_cap_amn
                 ,tot_drawdown_amn
                 ,tot_pnl_int_amn
                 ,tot_nrm_int_amn
                 ,tot_cap_amn
                 ,tot_commission_amn
                 ,lst_int_db_amn)
      SELECT   a.acc_unit
              ,a.acc_type
              ,a.acc_sn
              ,NVL( SUM (a.tot_pub_comm_amn), 0)
              ,NVL( SUM (a.tot_int_sprd_amn), 0)
              ,NVL( SUM (a.tot_thrdprt_amn), 0)
              ,NVL( SUM (a.mp_start_cap_amn), 0)
              ,NVL( SUM (a.tot_drawdown_amn), 0)
              ,NVL( SUM (a.tot_pnl_int_amn), 0)
              ,NVL( SUM (a.tot_nrm_int_amn), 0)
              ,NVL( SUM (a.tot_cap_amn), 0)
              ,NVL( SUM (a.tot_commission_amn), 0)
              ,NVL( SUM (a.lst_int_db_amn), 0)
      FROM     loan_trx_recording a, r_loan_lst_tmstamp b, bank_parameters c
      WHERE        a.trx_date = c.scheduled_date
               AND a.tmstamp > b.lst_tmstamp
               AND a.trx_internal_sn = 1
      GROUP BY a.acc_unit, a.acc_type, a.acc_sn;
   DELETE tmp_refresh_lns_nrm;
   INSERT INTO tmp_refresh_lns_nrm (
                  acc_unit
                 ,acc_type
                 ,acc_sn
                 ,capital_amn
                 ,expense_amn
                 ,commission_amn
                 ,rl_nrm_int_amn
                 ,url_nrm_int_amn
                 ,acr_nrm_int_amn
                 ,url_pub_int_amn
                 ,acr_pub_int_amn
                 ,subsidy_amn)
      SELECT   a.acc_unit
              ,a.acc_type
              ,a.acc_sn
              ,NVL( SUM (a.capital_amn), 0)
              ,NVL( SUM (a.expense_amn), 0)
              ,NVL( SUM (a.commission_amn), 0)
              ,NVL( SUM (a.rl_nrm_int_amn), 0)
              ,NVL( SUM (a.url_nrm_int_amn), 0)
              ,NVL( SUM (a.acr_nrm_int_amn), 0)
              ,NVL( SUM (a.url_pub_int_amn), 0)
              ,NVL( SUM (a.acr_pub_int_amn), 0)
              ,NVL( SUM (a.subsidy_amn), 0)
      FROM     loan_trx_recording a, r_loan_lst_tmstamp b, bank_parameters c
      WHERE        a.trx_date = c.scheduled_date
               AND a.tmstamp > b.lst_tmstamp
               AND a.request_loan_sts = '1'
      GROUP BY a.acc_unit, a.acc_type, a.acc_sn;
   DELETE tmp_refresh_lns_ov;
   INSERT INTO tmp_refresh_lns_ov (
                  acc_unit
                 ,acc_type
                 ,acc_sn
                 ,capital_amn
                 ,expense_amn
                 ,commission_amn
                 ,rl_nrm_int_amn
                 ,url_nrm_int_amn
                 ,acr_nrm_int_amn
                 ,url_pub_int_amn
                 ,acr_pub_int_amn
                 ,subsidy_amn)
      SELECT   a.acc_unit
              ,a.acc_type
              ,a.acc_sn
              ,NVL( SUM (a.capital_amn), 0)
              ,NVL( SUM (a.expense_amn), 0)
              ,NVL( SUM (a.commission_amn), 0)
              ,NVL( SUM (a.rl_nrm_int_amn), 0)
              ,NVL( SUM (a.url_nrm_int_amn), 0)
              ,NVL( SUM (a.acr_nrm_int_amn), 0)
              ,NVL( SUM (a.url_pub_int_amn), 0)
              ,NVL( SUM (a.acr_pub_int_amn), 0)
              ,NVL( SUM (a.subsidy_amn), 0)
      FROM     loan_trx_recording a, r_loan_lst_tmstamp b, bank_parameters c
      WHERE        a.trx_date = c.scheduled_date
               AND a.tmstamp > b.lst_tmstamp
               AND a.request_loan_sts > '1'
      GROUP BY a.acc_unit, a.acc_type, a.acc_sn;
   DELETE tmp_refresh_lns;
   INSERT INTO tmp_refresh_lns (
                  acc_unit
                 ,acc_type
                 ,acc_sn
                 ,rl_pnl_int_amn
                 ,url_pnl_int_amn
                 ,acr_pnl_int_amn
                 ,tot_exp_in_cc_amn
                 ,tot_com_in_cc_amn
                 ,thrdprt_amn
                 ,tot_subs_int_amn
                 ,tot_expense_amn
                 ,tot_confirm_amn
                 ,positive_amn
                 ,unclear_amn
                 ,blocked_amn
                 ,dormant_amn
                 ,last_nrm_trx_cnt
                 ,request_sn
                 ,install_round_amn
                 ,tot_pub_comm_amn
                 ,tot_int_sprd_amn
                 ,tot_thrdprt_amn
                 ,mp_start_cap_amn
                 ,tot_drawdown_amn
                 ,tot_pnl_int_amn
                 ,tot_nrm_int_amn
                 ,tot_cap_amn
                 ,tot_commission_amn
                 ,lst_int_db_amn
                 ,capital_amn
                 ,expense_amn
                 ,commission_amn
                 ,rl_nrm_int_amn
                 ,url_nrm_int_amn
                 ,acr_nrm_int_amn
                 ,url_pub_int_amn
                 ,acr_pub_int_amn
                 ,subsidy_amn
                 ,o_capital_amn
                 ,o_expense_amn
                 ,o_commission_amn
                 ,o_rl_nrm_int_amn
                 ,o_url_nrm_int_amn
                 ,o_acr_nrm_int_amn
                 ,o_url_pub_int_amn
                 ,o_acr_pub_int_amn
                 ,o_subsidy_amn
                 ,counter
                 ,max_req_loan_status)
      SELECT a.acc_unit
            ,a.acc_type
            ,a.acc_sn
            ,NVL (a.rl_pnl_int_amn, 0)
            ,NVL (a.url_pnl_int_amn, 0)
            ,NVL (a.acr_pnl_int_amn, 0)
            ,NVL (a.tot_exp_in_cc_amn, 0)
            ,NVL (a.tot_com_in_cc_amn, 0)
            ,NVL (a.thrdprt_amn, 0)
            ,NVL (a.tot_subs_int_amn, 0)
            ,NVL (a.tot_expense_amn, 0)
            ,NVL (a.tot_confirm_amn, 0)
            ,NVL (a.positive_amn, 0)
            ,NVL (a.unclear_amn, 0)
            ,NVL (a.blocked_amn, 0)
            ,NVL (a.dormant_amn, 0)
            ,NVL (a.last_nrm_trx_cnt, 0)
            ,NVL (a.request_sn, 0)
            ,NVL (a.install_round_amn, 0)
            ,NVL (b.tot_pub_comm_amn, 0)
            ,NVL (b.tot_int_sprd_amn, 0)
            ,NVL (b.tot_thrdprt_amn, 0)
            ,NVL (b.mp_start_cap_amn, 0)
            ,NVL (b.tot_drawdown_amn, 0)
            ,NVL (b.tot_pnl_int_amn, 0)
            ,NVL (b.tot_nrm_int_amn, 0)
            ,NVL (b.tot_cap_amn, 0)
            ,NVL (b.tot_commission_amn, 0)
            ,NVL (b.lst_int_db_amn, 0)
            ,NVL (c.capital_amn, 0)
            ,NVL (c.expense_amn, 0)
            ,NVL (c.commission_amn, 0)
            ,NVL (c.rl_nrm_int_amn, 0)
            ,NVL (c.url_nrm_int_amn, 0)
            ,NVL (c.acr_nrm_int_amn, 0)
            ,NVL (c.url_pub_int_amn, 0)
            ,NVL (c.acr_pub_int_amn, 0)
            ,NVL (c.subsidy_amn, 0)
            ,NVL (d.capital_amn, 0)
            ,NVL (d.expense_amn, 0)
            ,NVL (d.commission_amn, 0)
            ,NVL (d.rl_nrm_int_amn, 0)
            ,NVL (d.url_nrm_int_amn, 0)
            ,NVL (d.acr_nrm_int_amn, 0)
            ,NVL (d.url_pub_int_amn, 0)
            ,NVL (d.acr_pub_int_amn, 0)
            ,NVL (d.subsidy_amn, 0)
            ,a.counter
            ,a.max_req_loan_status
      FROM   tmp_refresh_lns_inst_1 a
             LEFT JOIN tmp_refresh_lns_inst b
                ON     a.acc_unit = b.acc_unit
                   AND a.acc_type = b.acc_type
                   AND a.acc_sn = b.acc_sn
             LEFT JOIN tmp_refresh_lns_nrm c
                ON     a.acc_unit = c.acc_unit
                   AND a.acc_type = c.acc_type
                   AND a.acc_sn = c.acc_sn
             LEFT JOIN tmp_refresh_lns_ov d
                ON     a.acc_unit = d.acc_unit
                   AND a.acc_type = d.acc_type
                   AND a.acc_sn = d.acc_sn;
   MERGE INTO r_loan_account a
   USING      (SELECT acc_unit
                     ,acc_type
                     ,acc_sn
                     ,rl_pnl_int_amn
                     ,url_pnl_int_amn
                     ,acr_pnl_int_amn
                     ,tot_exp_in_cc_amn
                     ,tot_com_in_cc_amn
                     ,tot_subs_int_amn
                     ,tot_expense_amn
                     ,tot_confirm_amn
                     ,positive_amn
                     ,unclear_amn
                     ,blocked_amn
                     ,dormant_amn
                     ,last_nrm_trx_cnt
                     ,request_sn
                     ,install_round_amn
                     ,tot_pub_comm_amn
                     ,tot_int_sprd_amn
                     ,tot_thrdprt_amn
                     ,mp_start_cap_amn
                     ,tot_drawdown_amn
                     ,tot_pnl_int_amn
                     ,tot_nrm_int_amn
                     ,tot_cap_amn
                     ,tot_commission_amn
                     ,lst_int_db_amn
                     ,capital_amn
                     ,expense_amn
                     ,commission_amn
                     ,rl_nrm_int_amn
                     ,url_nrm_int_amn
                     ,acr_nrm_int_amn
                     ,url_pub_int_amn
                     ,acr_pub_int_amn
                     ,subsidy_amn
                     ,o_capital_amn
                     ,o_expense_amn
                     ,o_commission_amn
                     ,o_rl_nrm_int_amn
                     ,o_url_nrm_int_amn
                     ,o_acr_nrm_int_amn
                     ,o_url_pub_int_amn
                     ,o_acr_pub_int_amn
                     ,o_subsidy_amn
                     ,max_req_loan_status
                     ,counter
               FROM   tmp_refresh_lns) b
   ON         (    a.fk_unitcode = b.acc_unit
               AND a.acc_type = b.acc_type
               AND a.acc_sn = b.acc_sn)
   WHEN MATCHED
   THEN
      UPDATE SET
         a.ov_rl_pnl_int_bal = a.ov_rl_pnl_int_bal + b.rl_pnl_int_amn
        ,a.ov_url_pnl_int_bal = a.ov_url_pnl_int_bal + b.url_pnl_int_amn
        ,a.ov_acr_pnl_int_bal = a.ov_acr_pnl_int_bal + b.acr_pnl_int_amn
        ,a.tot_exp_in_cc_amn = a.tot_exp_in_cc_amn + b.tot_exp_in_cc_amn
        ,a.tot_com_in_cc_amn = a.tot_com_in_cc_amn + b.tot_com_in_cc_amn
        ,a.tot_subs_int_amn = a.tot_subs_int_amn + b.tot_subs_int_amn
        ,a.tot_expense_amn = a.tot_expense_amn + b.tot_expense_amn
        ,a.tot_confirm_amn = a.tot_confirm_amn + b.tot_confirm_amn
        ,a.lst_trx_dt = (SELECT scheduled_date FROM bank_parameters)
        ,a.last_nrm_trx_cnt = b.last_nrm_trx_cnt
        ,a.req_install_sn = b.request_sn
        ,a.loan_status =
            DECODE (b.counter, 0, a.loan_status, b.max_req_loan_status)
        ,a.tot_pub_comm_amn = a.tot_pub_comm_amn + b.tot_pub_comm_amn
        ,a.tot_int_sprd_amn = a.tot_int_sprd_amn + b.tot_int_sprd_amn
        ,a.tot_thrdprt_amn = a.tot_thrdprt_amn + b.tot_thrdprt_amn
        ,a.mp_start_cap_amn = a.mp_start_cap_amn + b.mp_start_cap_amn
        ,a.tot_drawdown_amn = a.tot_drawdown_amn + b.tot_drawdown_amn
        ,a.tot_pnl_int_amn = a.tot_pnl_int_amn + b.tot_pnl_int_amn
        ,a.tot_nrm_int_amn = a.tot_nrm_int_amn + b.tot_nrm_int_amn
        ,a.tot_cap_amn = a.tot_cap_amn + b.tot_cap_amn
        ,a.tot_commission_amn = a.tot_commission_amn + b.tot_commission_amn
        ,a.nrm_cap_bal = a.nrm_cap_bal + b.capital_amn
        ,a.ov_cap_bal = a.ov_cap_bal + b.o_capital_amn
        ,a.nrm_exp_bal = a.nrm_exp_bal + b.expense_amn
        ,a.ov_exp_bal = a.ov_exp_bal + b.o_expense_amn
        ,a.nrm_com_bal = a.nrm_com_bal + b.commission_amn
        ,a.ov_com_bal = a.ov_com_bal + b.o_commission_amn
        ,a.nrm_rl_int_bal = a.nrm_rl_int_bal + b.rl_nrm_int_amn
        ,a.ov_rl_nrm_int_bal = a.ov_rl_nrm_int_bal + b.o_rl_nrm_int_amn
        ,a.nrm_url_int_bal = a.nrm_url_int_bal + b.url_nrm_int_amn
        ,a.ov_url_nrm_int_bal = a.ov_url_nrm_int_bal + b.o_url_nrm_int_amn
        ,a.nrm_acr_int_bal = a.nrm_acr_int_bal + b.acr_nrm_int_amn
        ,a.ov_acr_nrm_int_bal = a.ov_acr_nrm_int_bal + b.o_acr_nrm_int_amn
        ,a.nr_url_pub_int_amn = a.nr_url_pub_int_amn + b.url_pub_int_amn
        ,a.ov_url_pub_int_amn = a.ov_url_pub_int_amn + b.o_url_pub_int_amn
        ,a.nr_acr_pub_int_amn = a.nr_acr_pub_int_amn + b.acr_pub_int_amn
        ,a.ov_acr_pub_int_amn = a.ov_acr_pub_int_amn + b.o_acr_pub_int_amn
        ,a.nrm_subsidy_bal = a.nrm_subsidy_bal + b.subsidy_amn
        ,a.ov_subsidy_bal = a.ov_subsidy_bal + b.o_subsidy_amn;
   MERGE INTO r_loan_account_inf a
   USING      (SELECT acc_unit
                     ,acc_type
                     ,acc_sn
                     ,positive_amn
                     ,unclear_amn
                     ,blocked_amn
                     ,dormant_amn
                     ,install_round_amn
               FROM   tmp_refresh_lns) b
   ON         (    a.fk_loan_accountfk = b.acc_unit
               AND a.fk0loan_accountacc = b.acc_type
               AND a.fk_loan_accountacc = b.acc_sn)
   WHEN MATCHED
   THEN
      UPDATE SET a.positive_amn = a.positive_amn + b.positive_amn
                ,a.unclear_amn = a.unclear_amn + b.unclear_amn
                ,a.blocked_amn = a.blocked_amn + b.blocked_amn
                ,a.dormant_amn = a.dormant_amn + b.dormant_amn
                ,a.install_round_amn = a.install_round_amn + b.install_round_amn;
   MERGE INTO r_loan_account_inf a
   USING      (SELECT fk_loan_accountfk
                     ,fk0loan_accountacc
                     ,fk_loan_accountacc
                     ,nrm_accrual_amn
                     ,ov_accrual_amn
                     ,hold_nrm_rl_accr
                     ,hold_ov_rl_accr
                     ,unclear_amn
                     ,nrm_rl_url_flg
                     ,ov_rl_url_flg
                     ,provision_amount
                     ,loan_class
                     ,loan_sub_class
                     ,recoveries_dt
               FROM   loan_account_info) b
   ON         (    a.fk_loan_accountfk = b.fk_loan_accountfk
               AND a.fk0loan_accountacc = b.fk0loan_accountacc
               AND a.fk_loan_accountacc = b.fk_loan_accountacc)
   WHEN MATCHED
   THEN
      UPDATE SET a.nrm_accrual_amn = b.nrm_accrual_amn
                ,a.ov_accrual_amn = b.ov_accrual_amn
                ,a.hold_nrm_rl_accr = b.hold_nrm_rl_accr
                ,a.hold_ov_rl_accr = b.hold_ov_rl_accr
                ,a.unclear_amn = b.unclear_amn
                ,a.nrm_rl_url_flg = b.nrm_rl_url_flg
                ,a.ov_rl_url_flg = b.ov_rl_url_flg
                ,a.provision_amount = b.provision_amount
                ,a.loan_class = b.loan_class
                ,a.loan_sub_class = b.loan_sub_class
                ,a.recoveries_dt = b.recoveries_dt;
END;

