CREATE VIEW w_stg_deposit_account
AS
   WITH exc
        AS (SELECT   MIN (percentage) excess_interest_rate
                    ,r_deposit_account.account_number dep_acc_number
            FROM     r_deposit_account
                     JOIN stat_account_bal
                        ON stat_account_bal.account_number =
                              r_deposit_account.account_number
                     JOIN int_scale
                        ON (    int_scale.fk_interestid_inte =
                                   r_deposit_account.fk_interestid_inte
                            AND int_scale.fk_currencyid_curr =
                                   r_deposit_account.fk_currencyid_curr)
            WHERE        stat_account_bal.book_balance < 0
                     AND ABS (stat_account_bal.book_balance) > account_limit
                     AND int_scale.validity_date <=
                            (SELECT scheduled_date FROM bank_parameters)
                     AND -1 * (stat_account_bal.book_balance + account_limit) BETWEEN from_amount
                                                                                  AND to_amount
                     AND int_scale.int_rate_tab_no <> 0
                     AND int_scale.fk_interestid_inte IS NOT NULL
            GROUP BY r_deposit_account.account_number)
       ,deb
        AS (SELECT   MIN (percentage) debit_interest_rate
                    ,r_deposit_account.account_number dep_acc_number
            FROM     r_deposit_account
                     JOIN stat_account_bal
                        ON stat_account_bal.account_number =
                              r_deposit_account.account_number
                     JOIN int_scale
                        ON (    int_scale.fk_interestid_inte =
                                   r_deposit_account.fk1interestid_inte
                            AND int_scale.fk_currencyid_curr =
                                   r_deposit_account.fk_currencyid_curr)
            WHERE        stat_account_bal.book_balance < 0
                     AND int_scale.validity_date <=
                            (SELECT scheduled_date FROM bank_parameters)
                     AND -1 * stat_account_bal.book_balance BETWEEN from_amount
                                                                AND to_amount
                     AND int_scale.int_rate_tab_no <> 0
                     AND int_scale.fk_interestid_inte IS NOT NULL
            GROUP BY r_deposit_account.account_number)
   SELECT profits_account.account_ser_num acct_key
         ,profits_account.dep_acc_number
         ,profits_account.account_number
         ,exc.excess_interest_rate
         ,deb.debit_interest_rate
         ,r_deposit_account.entry_status
         ,r_deposit_account.accr_excess_inter
         ,r_deposit_account.accr_exc_progress
         ,stat_account_bal.rate
         ,r_deposit_account.expiry_date
         ,profits_account0.account_number loan_account_number
         ,profits_account0.account_ser_num loan_acct_key
   FROM   r_deposit_account
          INNER JOIN profits_account
             ON r_deposit_account.account_number =
                   profits_account.dep_acc_number
          LEFT JOIN stat_account_bal
             ON stat_account_bal.account_number =
                   r_deposit_account.account_number
          LEFT JOIN dep_account_info
             ON (r_deposit_account.account_number =
                    dep_account_info.fk1deposit_accoacc)
          LEFT JOIN r_loan_account l
             ON (    dep_account_info.lns_acc_type = l.acc_type
                 AND dep_account_info.lns_unitcode = l.fk_unitcode
                 AND dep_account_info.lns_acc_sn = l.acc_sn
                 AND l.acc_type = 14)
          LEFT JOIN profits_account profits_account0
             ON (    profits_account0.lns_open_unit = l.fk_unitcode
                 AND profits_account0.lns_sn = l.acc_sn
                 AND profits_account0.lns_type = l.acc_type
                 AND profits_account0.prft_system = 4)
          LEFT JOIN exc
             ON exc.dep_acc_number = profits_account.dep_acc_number
          LEFT JOIN deb
             ON deb.dep_acc_number = profits_account.dep_acc_number
   WHERE  profits_account.prft_system = 3;

