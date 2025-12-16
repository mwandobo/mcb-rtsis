create table DP_MAJORTRANS_FILE
(
    MONTH0           SMALLINT not null,
    YEAR0            SMALLINT not null,
    CURRENCY         CHAR(5)  not null,
    TRANS_CNT        INTEGER,
    TOTAL_ACCT_CNT   INTEGER,
    TOTAL_AMT        DECIMAL(15, 2),
    TOT_AMT_CONV_USD DECIMAL(15, 2),
    constraint PMJRTRNS
        primary key (MONTH0, YEAR0, CURRENCY)
);

