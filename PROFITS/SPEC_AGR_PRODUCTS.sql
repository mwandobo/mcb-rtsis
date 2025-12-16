create table SPEC_AGR_PRODUCTS
(
    CATEGORY_CODE         CHAR(8) not null,
    FK_GH_TYPE            CHAR(5) not null,
    FK_GH_SN              INTEGER not null,
    ID_PRODUCT            INTEGER not null,
    ID_CURRENCY           INTEGER not null,
    SN                    INTEGER not null,
    CONTRIBUTION_AMN      DECIMAL(18, 2),
    CONTRIBUTION_FREQ     SMALLINT,
    REGISTRATION_FEE      DECIMAL(18, 2),
    REGISTRATION_FEE_CODE INTEGER,
    REJOIN_FEE            DECIMAL(18, 2),
    REJOIN_FEE_CODE       INTEGER,
    HAS_ACCOUNT_OPEN      CHAR(1),
    NO_OPEN_IF_ALREADY    CHAR(1),
    HAS_SWEEP_AGREEMENT   CHAR(1),
    ENTRY_COMMENTS        VARCHAR(80),
    ENTRY_STATUS          CHAR(1),
    CREATE_UNIT           INTEGER,
    CREATE_USER           CHAR(8),
    CREATE_DATE           DATE,
    CREATE_TMSTAMP        TIMESTAMP(6),
    UPDATE_UNIT           INTEGER,
    UPDATE_USER           CHAR(8),
    UPDATE_DATE           DATE,
    UPDATE_TMSTAMP        TIMESTAMP(6),
    constraint PK_SPEC_PRODS
        primary key (CATEGORY_CODE, FK_GH_TYPE, FK_GH_SN, ID_PRODUCT, ID_CURRENCY)
);

