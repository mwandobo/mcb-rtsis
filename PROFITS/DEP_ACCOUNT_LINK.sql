create table DEP_ACCOUNT_LINK
(
    INITIAL_ACCOUNT  DECIMAL(11) not null,
    NEW_ACCOUNT      DECIMAL(11) not null,
    INITIAL_CD       SMALLINT,
    NEW_CD           SMALLINT,
    INITIAL_ACC_UNIT INTEGER,
    NEW_ACC_UNIT     INTEGER,
    LINKAGE_DATE     DATE,
    TIMESTMP         DATE,
    PROCESSED_FLG    CHAR(1),
    constraint IXU_REP_047
        primary key (INITIAL_ACCOUNT, NEW_ACCOUNT)
);

