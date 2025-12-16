CREATE PROCEDURE refresh_r_deposit_balance
BEGIN
   FOR c_deposit_account
      AS (SELECT   account_number
          FROM     r_deposit_account
          WHERE    account_number <> 0 AND deposit_type IN (1, 5)
          ORDER BY account_number ASC)
   DO
      FOR c_hist_extrait
         AS (
              (SELECT debit_credit_flag, prev_acc_balance, entry_amount, bb
               FROM   (SELECT   ROW_NUMBER ()
                                OVER (
                                   ORDER BY
                                      trx_date DESC, trans_ser_num DESC, entry_ser_num DESC)
                                   rownumber
                               ,debit_credit_flag
                               ,prev_acc_balance
                               ,entry_amount
                               ,DECODE( debit_credit_flag
                                  , '1'
                                  , prev_acc_balance -  entry_amount
                                  , '2'
                                  , prev_acc_balance -  entry_amount
                                  , entry_amount
                                  ) AS bb
                       FROM     history_fd_extrait, bank_parameters bp
                       WHERE    account_number = c_deposit_account.account_number
                            AND trx_date = bp.prev_trx_date
                       ORDER BY trx_date DESC, trans_ser_num DESC, entry_ser_num DESC)
               WHERE  rownumber = 1)
             )
      DO
            UPDATE r_deposit_account
            SET    book_balance = c_hist_extrait.bb
                  ,available_balance =
                      c_hist_extrait.bb - unclear_balance - blocked_balance
            WHERE  r_deposit_account.account_number = c_deposit_account.account_number;
      END FOR;
   END FOR;
   FOR c_deposit_account_2 AS (SELECT   account_number
                               FROM     deposit_account
                               WHERE    account_number <> 0
                               ORDER BY account_number ASC)
   DO
      FOR c_update_r_dep_acc_22 AS (SELECT accr_n128_inter
                                          ,accr_n128_progess
                                          ,accr_exc_progress
                                          ,transition_excess
                                          ,transition_n128
                                          ,cr_progress_inter
                                          ,db_transition_inte
                                          ,db_transit_int_tax
                                          ,accr_excess_inter
                                          ,accr_excess_int
                                          ,accr_cr_exp_int
                                          ,transition_inter
                                          ,transition_int_tax
                                          ,accr_cr_interest
                                          ,accr_db_interest
                                          ,db_progress_inter
                                    FROM   deposit_account
                                    WHERE  account_number = c_deposit_account_2.account_number)
      DO
         UPDATE r_deposit_account
         SET    accr_n128_inter = c_update_r_dep_acc_22.accr_n128_inter
               ,accr_n128_progess = c_update_r_dep_acc_22.accr_n128_progess
               ,accr_exc_progress = c_update_r_dep_acc_22.accr_exc_progress
               ,transition_excess = c_update_r_dep_acc_22.transition_excess
               ,transition_n128 = c_update_r_dep_acc_22.transition_n128
               ,cr_progress_inter = c_update_r_dep_acc_22.cr_progress_inter
               ,db_transition_inte = c_update_r_dep_acc_22.db_transition_inte
               ,db_transit_int_tax = c_update_r_dep_acc_22.db_transit_int_tax
               ,accr_excess_inter = c_update_r_dep_acc_22.accr_excess_inter
               ,accr_excess_int = c_update_r_dep_acc_22.accr_excess_int
               ,accr_cr_exp_int = c_update_r_dep_acc_22.accr_cr_exp_int
               ,transition_inter = c_update_r_dep_acc_22.transition_inter
               ,transition_int_tax = c_update_r_dep_acc_22.transition_int_tax
               ,accr_cr_interest = c_update_r_dep_acc_22.accr_cr_interest
               ,accr_db_interest = c_update_r_dep_acc_22.accr_db_interest
               ,db_progress_inter = c_update_r_dep_acc_22.db_progress_inter
         WHERE  account_number = c_deposit_account_2.account_number;
      END FOR;
   END FOR;
END;

