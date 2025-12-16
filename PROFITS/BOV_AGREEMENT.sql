create table BOV_AGREEMENT
(
    AGREEMENT_NUMBER               DECIMAL(10) not null
        constraint PK_AGREEMENT
            primary key,
    AGREEMENT_TYPE                 CHAR(2),
    C_DIGIT                        SMALLINT,
    AGREEMENT_LIMIT                DECIMAL(15, 2),
    UTILISED_LIMIT                 DECIMAL(15, 2),
    AMENDMENT_COUNT                SMALLINT,
    ISSUE_DATE                     DATE,
    OLD_AGREEMENT_NUM              CHAR(8),
    CURRENCY_INDICATOR             CHAR(1),
    APPROVAL_DATE                  DATE,
    APPROVAL_NUMBER                CHAR(12),
    LAST_UPDATE_DATE               DATE,
    FK_CUSTOMERCUST_ID             INTEGER,
    FK_CURRENCYID_CURR             INTEGER,
    PREV_AGREEM_LIMIT              DECIMAL(15, 2),
    ENTRY_COMMENTS                 CHAR(40),
    ENTRY_STATUS                   CHAR(1),
    TIMESTMP                       TIMESTAMP(6),
    FK_CUST_ADDRESSFK_CUSTOMERCUST INTEGER,
    FK_CUST_ADDRESSSERIAL_NUM      SMALLINT
);

