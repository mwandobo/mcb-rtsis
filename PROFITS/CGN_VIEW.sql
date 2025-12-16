create table CGN_VIEW
(
    VIEW_SN           DECIMAL(10)  not null
        constraint PK_VIEW
            primary key,
    TYPES             CHAR(1),
    PUBLIC_VIEW       CHAR(1)      not null,
    EXTERNAL_VIEW     CHAR(1),
    VIEW_CREATE_OWNER CHAR(8)      not null,
    ACTUAL_SPENDING   DECIMAL(18),
    CUSTOMER_COUNT    DECIMAL(10),
    ACCOUNT_COUNT     DECIMAL(10),
    RESULT_DATE       DATE,
    LEADS_COUNT       DECIMAL(10),
    MKT_PLAN_ID       CHAR(10),
    MKT_PLAN_SN       INTEGER,
    GL_ID_CURRENCY    INTEGER,
    ENTRY_STATUS      CHAR(1)      not null,
    TMSTAMP           TIMESTAMP(6) not null,
    DESCRIPTION       VARCHAR(100),
    NOTES_MEMO        VARCHAR(100),
    DB_VIEW_NAME      VARCHAR(25),
    DB_VIEW_SCRIPT    VARCHAR(4000),
    DB_VIEW_EXEC      VARCHAR(200)
);

