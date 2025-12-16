create table CLC_INT_TRANSACTIONS
(
    TRN_ID                     VARCHAR(32)    not null,
    ACC_ID                     VARCHAR(17)    not null,
    TRN_CODE                   VARCHAR(32)    not null,
    TRN_DATE                   DATE           not null,
    TRN_POSTDATE               DATE           not null,
    TRN_CURRENCY               CHAR(3)        not null,
    TRN_AMOUNT                 DECIMAL(18, 3) not null,
    TRN_COMMENT                VARCHAR(150),
    TRN_DATADATE               DATE           not null,
    TRN_EXPORTDATE             DATE           not null,
    TMSTAMP_LOAN_ACCOUNT       TIMESTAMP(6),
    TMSTAMP_LOAN_ACCOUNT_EXTRA TIMESTAMP(6),
    TRN_CURRENT_BALANCE        DECIMAL(18, 3),
    constraint PK_CLC_INT_TRANSAC
        primary key (TRN_EXPORTDATE, TRN_ID)
);

comment on table CLC_INT_TRANSACTIONS is 'This file contains all transactions of the accounts included in the Accounts file registered on TRN_DataDate.';

comment on column CLC_INT_TRANSACTIONS.TRN_ID is 'Transaction Unique ID - For loan_accounts ->   loan_account_extra trx_unit-trx_date-trx_usr-trx_sn For deposit_accounts -> fst_demand_extrait.trx_unit-trx_date-trx_usr-trans_ser_num';

comment on column CLC_INT_TRANSACTIONS.ACC_ID is 'Account Number - PROFITS_ACCOUNT.ACCOUNT_NUMBER';

comment on column CLC_INT_TRANSACTIONS.TRN_CODE is 'For loan_accounts -> loan_account_extra.TRANSACTION_CODEFor deposit_accounts -> fst_demand_extrait.ID_TRANSACT';

comment on column CLC_INT_TRANSACTIONS.TRN_DATE is 'Transaction Date - For loan_accounts -> loan_account_extra.trx_dateFor deposit_accounts -> fst_demand_extrait.TRX_DATE';

comment on column CLC_INT_TRANSACTIONS.TRN_POSTDATE is 'Value Date - For loan_accounts -> loan_account_extra.valeur_dtFor deposit_accounts -> fst_demand_extrait.VALUE_DATE';

comment on column CLC_INT_TRANSACTIONS.TRN_CURRENCY is 'Currency of transaction (ISO Standard) - For loan_accounts -> loan_account_extra. trx_currFor deposit_accounts -> fst_demand_extrait.trx_curr';

comment on column CLC_INT_TRANSACTIONS.TRN_AMOUNT is 'Amount    - For loan_accounts -> loan_account_extra.TRX_AMNFor deposit_accounts -> fst_demand_extrait.IN_AMOUNT';

comment on column CLC_INT_TRANSACTIONS.TRN_COMMENT is 'For loan_accounts -> loan_account_extra.EXTRAIT_COMMENTSFor deposit_accounts -> fst_demand_extrait.ENTRY_COMMENTS';

comment on column CLC_INT_TRANSACTIONS.TRN_DATADATE is 'Date that data refer to - FILLED BY EXPORT APP';

comment on column CLC_INT_TRANSACTIONS.TRN_EXPORTDATE is 'Export date - FILLED BY EXPORT APP';

