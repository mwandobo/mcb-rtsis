create table CLC_INT_BUCKETS
(
    CCBA_ID              VARCHAR(23)    not null,
    ACC_ID               VARCHAR(17)    not null,
    CCBA_BUCKETNO        VARCHAR(5)     not null,
    CCBA_TOTALAMOUNT     DECIMAL(18, 3) not null,
    CCBA_CAPITALAMT      DECIMAL(18, 3),
    CCBA_INTERESTAMT     DECIMAL(18, 3),
    CCBA_FROZENAMT       DECIMAL(18, 3),
    CCBA_OVRDINTAMT      DECIMAL(18, 3),
    CCBA_CURRCODE        CHAR(3),
    CCBA_DATADATE        DATE           not null,
    CCBA_EXPORTDATE      DATE           not null,
    TMSTAMP_LOAN_ACCOUNT TIMESTAMP(6),
    constraint PK_CLC_INT_BUCKETS
        primary key (CCBA_EXPORTDATE, CCBA_ID)
);

comment on table CLC_INT_BUCKETS is 'This table contains analytic data for the whole overdue amount of an account, based on most recent Billing Date. One record will be included per each combination of Collection Account and Billing Date.Accounts that are included in this file are all accou';

comment on column CLC_INT_BUCKETS.CCBA_ID is 'Account number + billing date (YYMMDD) - PROFITS_ACCOUNT.ACCOUNT_NUMBER + LOAN_REQUEST..REQUEST_EXPDT WHERE LOAN_REQUEST..REQUEST.STATUS = 1 AND LOAN_REQUEST..REQUEST_TYPE <> 3';

comment on column CLC_INT_BUCKETS.ACC_ID is 'Account number - PROFITS_ACCOUNT.ACCOUNT_NUMBER';

comment on column CLC_INT_BUCKETS.CCBA_BUCKETNO is 'Bucket Number - CASE VIA SQL USING BANK_PARAMETERS.CURR_TRX -  LOAN_REQUEST.REQUEST_EXP_DT';

comment on column CLC_INT_BUCKETS.CCBA_TOTALAMOUNT is 'Total Amount -';

comment on column CLC_INT_BUCKETS.CCBA_CAPITALAMT is 'Capital Amount () -';

comment on column CLC_INT_BUCKETS.CCBA_INTERESTAMT is 'Interest Amount and Fees (  ) - LOAN_REQUEST.RL_INTEREST +  LOAN_REQUEST.EXPENSES_AMN LOAN_REQUEST.COMM_AMN';

comment on column CLC_INT_BUCKETS.CCBA_FROZENAMT is 'Frozen Amount (   ) - LOAN_REQUEST.URL_INTEREST';

comment on column CLC_INT_BUCKETS.CCBA_OVRDINTAMT is 'Overdue Interest Amount ( ) - LOAN_REQUEST.RL_INTEREST +  LOAN_REQUEST.URL_INTEREST + LOAN_REQUEST.PNL_INTEREST';

comment on column CLC_INT_BUCKETS.CCBA_CURRCODE is 'Currency (ISO Standard). If empty Account Currency is assumed. - PROFITS_ACCOUNT.MOVEMENT_CURRENCY';

comment on column CLC_INT_BUCKETS.CCBA_DATADATE is 'Date that data refer to - FILLED BY EXPORT APP';

comment on column CLC_INT_BUCKETS.CCBA_EXPORTDATE is 'Export date - FILLED BY EXPORT APP';

