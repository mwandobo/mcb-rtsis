create table DEFAULT_TRIPLET
(
    PRFT_SYSTEM SMALLINT not null,
    TYPE        CHAR(1)  not null,
    SUB_TYPE    INTEGER  not null,
    ID_TRANSACT INTEGER  not null,
    ID_JUSTIFIC INTEGER  not null,
    PR_ID_1     INTEGER,
    PR_ID_3     INTEGER,
    PR_ID_4     INTEGER,
    AR_ID_1     INTEGER,
    AR_ID_2     INTEGER,
    AR_ID_3     INTEGER,
    AR_ID_4     INTEGER,
    PR_ID_2     INTEGER,
    HOME_BRANCH CHAR(1),
    constraint IXU_DEF_000
        primary key (PRFT_SYSTEM, TYPE, SUB_TYPE, ID_TRANSACT, ID_JUSTIFIC)
);

