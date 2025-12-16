create table TRS_MM_DISTR_HDR
(
    DISTR_SN               INTEGER        not null,
    DISTR_STS              CHAR(1)        not null,
    DISTR_AMOUNT           DECIMAL(15, 2) not null,
    DISTR_DATE             DATE           not null,
    CANCEL_USER            CHAR(8),
    CANCEL_DATE            DATE,
    TMSTAMP                TIMESTAMP(6)   not null,
    FK_MM_DEAL_NO          INTEGER        not null,
    FK_CURRENCYID_CURRENCY INTEGER,
    constraint TRS_DISTR
        primary key (FK_MM_DEAL_NO, DISTR_SN)
);

comment on table TRS_MM_DISTR_HDR is 'Holds created outgoing payments of syndicated corporate loans for distribution.';

comment on column TRS_MM_DISTR_HDR.DISTR_STS is 'Holds the distribution status:0: Cancelled1: Pending2: Distributed';

