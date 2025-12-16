CREATE VIEW W_FACT_FX_FT_TRX
AS
   WITH t
        AS (SELECT trx_usr trx_user,
                   trx_unit,
                   trx_date,
                   trx_sn trx_sn,
                   tun_internal_sn,
                   fx_ft_recording.tmstamp,
                   fx_ft_recording.product_code,
                   fx_ft_recording.trx_code,
                   fx_ft_recording.justification justification_code,
                   channel_id,
                   fx_ft_recording.source_currency,
                   fx_ft_recording.target_currency,
                   fx_ft_recording.com_cha_tax_curr com_cha_tax_currency,
                   fx_ft_recording.trx_drawdown_sn,
                   fx_ft_recording.trx_payment_sn,
                   DECODE (NVL (fx_ft_recording.org_trx_unit, 0),
                           0, 'Original',
                           'Reversal')
                      reversal_flag,
                   DECODE (fx_ft_recording.transaction_status,
                           '1', 'Reversed',
                           'Posted')
                      transaction_status_flag,
                   DECODE (fx_ft_recording.gl_article_code,
                           0, 'Non-financial',
                           'Financial')
                      financial_flag,
                   DECODE (
                      NVL (org_trx_unit, 0),
                      0, ' ',
                         TO_CHAR (org_trx_date, 'yyyy-mm-dd')
                      || '|'
                      || org_trx_unit
                      || '|'
                      || TRIM (org_trx_usr)
                      || '|'
                      || org_trx_sn)
                      reversed_tun,
                   CASE
                      WHEN     fx_ft_recording.source_trn_type IN ('1', '7')
                           AND fx_ft_recording.source_currency != 0
                      THEN
                         fx_ft_recording.source_u_user_tota
                      ELSE
                         0
                   END
                      source_cash_debit_amt,
                   CASE
                      WHEN     fx_ft_recording.target_trn_type IN ('1', '7')
                           AND fx_ft_recording.target_currency != 0
                      THEN
                         fx_ft_recording.target_u_user_tota
                      ELSE
                         0
                   END
                      target_cash_debit_amt,
                   CASE
                      WHEN     fx_ft_recording.source_trn_type IN ('2', '8')
                           AND fx_ft_recording.source_currency != 0
                      THEN
                         fx_ft_recording.source_u_user_tota
                      ELSE
                         0
                   END
                      source_cash_credit_amt,
                   CASE
                      WHEN     fx_ft_recording.target_trn_type IN ('2', '8')
                           AND fx_ft_recording.target_currency != 0
                      THEN
                         fx_ft_recording.target_u_user_tota
                      ELSE
                         0
                   END
                      target_cash_credit_amt,
                   CASE
                      WHEN    (    fx_ft_recording.source_currency != 0
                               AND fx_ft_recording.source_trn_type IN ('3',
                                                                       'A',
                                                                       'B',
                                                                       'C',
                                                                       'D',
                                                                       'E'))
                           OR (    fx_ft_recording.trx_code = 12501
                               AND justific.for_atm != '1'
                               AND fx_ft_recording.source_dep_account != 0)
                      THEN
                         fx_ft_recording.source_u_user_tota
                      ELSE
                         0
                   END
                      source_journal_debit_amt,
                   CASE
                      WHEN    (    fx_ft_recording.target_currency != 0
                               AND fx_ft_recording.target_trn_type IN ('3',
                                                                       'A',
                                                                       'B',
                                                                       'C',
                                                                       'D',
                                                                       'E'))
                           OR (    fx_ft_recording.trx_code = 12501
                               AND justific.for_atm != '1'
                               AND fx_ft_recording.target_dep_account != 0)
                      THEN
                         fx_ft_recording.target_u_user_tota
                      ELSE
                         0
                   END
                      target_journal_debit_amt,
                   CASE
                      WHEN    (    fx_ft_recording.source_currency != 0
                               AND fx_ft_recording.source_trn_type IN ('4',
                                                                       'A',
                                                                       'B',
                                                                       'C',
                                                                       'D',
                                                                       'E'))
                           OR (    fx_ft_recording.trx_code = 12501
                               AND justific.for_atm != '1'
                               AND fx_ft_recording.source_dep_account != 0)
                      THEN
                         fx_ft_recording.source_u_user_tota
                      ELSE
                         0
                   END
                      source_journal_credit_amt,
                   CASE
                      WHEN    (    fx_ft_recording.target_currency != 0
                               AND fx_ft_recording.target_trn_type IN ('4',
                                                                       'A',
                                                                       'B',
                                                                       'C',
                                                                       'D',
                                                                       'E'))
                           OR (    fx_ft_recording.trx_code = 12501
                               AND justific.for_atm != '1'
                               AND fx_ft_recording.target_dep_account != 0)
                      THEN
                         fx_ft_recording.target_u_user_tota
                      ELSE
                         0
                   END
                      target_journal_credit_amt,
                   fx_ft_recording.source_dep_account,
                   fx_ft_recording.target_dep_account,
                       DECODE (org_trx_unit, 0, 1, -1)
                     * fx_ft_recording.acc_amount_30
                   - fx_ft_recording.acc_amount_30_ls
                   + fx_ft_recording.acc_amount_30_pr
                      com_cha_tax_cash_debit,
                       DECODE (org_trx_unit, 0, 1, -1)
                     * DECODE (exch_notes_flag, '1', 0, 1)
                     * fx_ft_recording.acc_amount_44
                   - fx_ft_recording.acc_amount_44_ls
                   + fx_ft_recording.acc_amount_44_pr
                      com_cha_tax_cash_credit,
                     acc_amount_5
                   + acc_amount_9
                   + acc_amount_13
                   - (acc_amount_7 + acc_amount_11 + acc_amount_15)
                      commission_amt,
                     acc_amount_6
                   + acc_amount_10
                   + acc_amount_14
                   - (acc_amount_8 + acc_amount_12 + acc_amount_16)
                      commission_lc_amt,
                   acc_amount_7 + acc_amount_11 + acc_amount_15
                      tax_on_commission_amt,
                   acc_amount_8 + acc_amount_12 + acc_amount_16
                      tax_on_commission_lc_amt,
                     acc_amount_17
                   + acc_amount_21
                   + acc_amount_25
                   - (acc_amount_19 + acc_amount_23 + acc_amount_27)
                      expense_amt,
                     acc_amount_18
                   + acc_amount_22
                   + acc_amount_26
                   - (acc_amount_20 + acc_amount_24 + acc_amount_28)
                      expense_lc_amt,
                   acc_amount_19 + acc_amount_23 + acc_amount_27
                      tax_on_expense_amt,
                   acc_amount_20 + acc_amount_24 + acc_amount_28
                      tax_on_expense_lc_amt,
                   authorizer1,
                   authorizer2
              FROM fx_ft_recording
                   LEFT JOIN justific
                      ON (justific.id_justific =
                             fx_ft_recording.justification)
                   LEFT JOIN bank_parameters ON (1 = 1)),
        y
        AS (SELECT 'Source' source_target_ind,
                   trx_user,
                   trx_unit,
                   trx_date,
                   trx_sn,
                   tun_internal_sn,
                   tmstamp,
                   product_code,
                   trx_code,
                   justification_code,
                   channel_id,
                   reversal_flag,
                   transaction_status_flag,
                   financial_flag,
                   reversed_tun,
                   source_cash_debit_amt cash_debit_amt, --TARGET_CASH_DEBIT_AMT,
                   source_cash_credit_amt cash_credit_amt, --TARGET_CASH_CREDIT_AMT,
                   source_journal_debit_amt journal_debit_amt, --TARGET_JOURNAL_DEBIT_AMT,
                   source_journal_credit_amt journal_credit_amt, --TARGET_JOURNAL_CREDIT_AMT,
                   --                   source_acct_key acct_key,
                   --TARGET_ACCT_KEY,
                   source_dep_account dep_acc_number,    --TARGET_DEP_ACCOUNT,
                   source_currency currency_id,
                   commission_amt,
                   commission_lc_amt,
                   tax_on_commission_amt,
                   tax_on_commission_lc_amt,
                   expense_amt,
                   expense_lc_amt,
                   tax_on_expense_amt,
                   tax_on_expense_lc_amt,                   --TARGET_CURRENCY,
                   --COM_CHA_TAX_CURRENCY,
                   --COM_CHA_TAX_CASH_DEBIT,
                   --COM_CHA_TAX_CASH_CREDIT
                   trx_drawdown_sn loan_trx_sn,
                   authorizer1,
                   authorizer2
              FROM t
            UNION ALL
            SELECT 'Target' source_target_ind,
                   trx_user,
                   trx_unit,
                   trx_date,
                   trx_sn,
                   tun_internal_sn,
                   tmstamp,
                   product_code,
                   trx_code,
                   justification_code,
                   channel_id,
                   reversal_flag,
                   transaction_status_flag,
                   financial_flag,
                   reversed_tun,                      --SOURCE_CASH_DEBIT_AMT,
                   target_cash_debit_amt,            --SOURCE_CASH_CREDIT_AMT,
                   target_cash_credit_amt,         --SOURCE_JOURNAL_DEBIT_AMT,
                   target_journal_debit_amt,      --SOURCE_JOURNAL_CREDIT_AMT,
                   target_journal_credit_amt,               --SOURCE_ACCT_KEY,
                   --                   target_acct_key,
                   --SOURCE_DEP_ACCOUNT,
                   target_dep_account dep_acc_number,        --SOURCE_CURRENCY
                   target_currency,                    --COM_CHA_TAX_CURRENCY,
                   --COM_CHA_TAX_CASH_DEBIT,
                   --COM_CHA_TAX_CASH_CREDIT
                   NULL commission_amt,
                   NULL commission_lc_amt,
                   NULL tax_on_commission_amt,
                   NULL tax_on_commission_lc_amt,
                   NULL expense_amt,
                   NULL expense_lc_amt,
                   NULL tax_on_expense_amt,
                   NULL tax_on_expense_lc_amt,
                   trx_payment_sn loan_trx_sn,
                   authorizer1,
                   authorizer2
              FROM t
            UNION ALL
            SELECT 'Charges' source_target_ind,
                   trx_user,
                   trx_unit,
                   trx_date,
                   trx_sn,
                   tun_internal_sn,
                   tmstamp,
                   product_code,
                   trx_code,
                   justification_code,
                   channel_id,
                   reversal_flag,
                   transaction_status_flag,
                   financial_flag,
                   reversed_tun,                      --SOURCE_CASH_DEBIT_AMT,
                   com_cha_tax_cash_debit cash_debit_amt, --SOURCE_CASH_CREDIT_AMT,
                   com_cha_tax_cash_credit cash_credit_amt, --SOURCE_JOURNAL_DEBIT_AMT,
                   0 target_journal_debit_amt,    --SOURCE_JOURNAL_CREDIT_AMT,
                   0 target_journal_credit_amt,             --SOURCE_ACCT_KEY,
                   --                   0 target_acct_key,
                   --SOURCE_DEP_ACCOUNT,
                   0 dep_account,                            --SOURCE_CURRENCY
                   --TARGET_CURRENCY,
                   com_cha_tax_currency,             --COM_CHA_TAX_CASH_DEBIT,
                   --COM_CHA_TAX_CASH_CREDIT
                   NULL commission_amt,
                   NULL commission_lc_amt,
                   NULL tax_on_commission_amt,
                   NULL tax_on_commission_lc_amt,
                   NULL expense_amt,
                   NULL expense_lc_amt,
                   NULL tax_on_expense_amt,
                   NULL tax_on_expense_lc_amt,
                   0 loan_trx_sn,
                   authorizer1,
                   authorizer2
              FROM t)
   SELECT y."SOURCE_TARGET_IND",
          y."TRX_USER",
          y."TRX_UNIT",
          y."TRX_DATE",
          y."TRX_SN",
          y."TUN_INTERNAL_SN",
          y."TMSTAMP",
          y."PRODUCT_CODE",
          y."TRX_CODE",
          y."JUSTIFICATION_CODE",
          y."CHANNEL_ID",
          y."REVERSAL_FLAG",
          y."TRANSACTION_STATUS_FLAG",
          y."FINANCIAL_FLAG",
          y."REVERSED_TUN",
          y."CASH_DEBIT_AMT",
          y."CASH_CREDIT_AMT",
          y."JOURNAL_DEBIT_AMT",
          y."JOURNAL_CREDIT_AMT",
          y."DEP_ACC_NUMBER",
          y."CURRENCY_ID",
          currency.short_descr currency,
          acc.account_ser_num acct_key,
          TRIM (acc.account_number) || '-' || acc.account_cd account_no,
          justific.description justification_name,
          prft_transaction.description transaction_name,
          commission_amt,
          commission_lc_amt,
          tax_on_commission_amt,
          tax_on_commission_lc_amt,
          expense_amt,
          expense_lc_amt,
          tax_on_expense_amt,
          tax_on_expense_lc_amt,
          loan_trx_sn,
          authorizer1,
          authorizer2
     FROM y
          LEFT JOIN currency ON currency.id_currency = currency_id
          LEFT JOIN profits_account acc
             ON (acc.dep_acc_number = y.dep_acc_number)
          LEFT JOIN justific ON justific.id_justific = y.justification_code
          LEFT JOIN prft_transaction
             ON prft_transaction.id_transact = y.trx_code
        where ACC.PRFT_SYSTEM = 3
   WITH NO ROW MOVEMENT;

