create table PTJ_MULTIPLE_DCD
(
    ID_PRODUCT          INTEGER not null,
    ID_TRANSACT         INTEGER not null,
    ID_JUSTIFIC         INTEGER not null,
    SERIAL_NUM          INTEGER not null,
    PRFT_SYSTEM         SMALLINT,
    RULE_ID             DECIMAL(12),
    EXCLUDE_COMBINATION CHAR(1),
    DESCRIPTION         CHAR(200),
    constraint PK_PTJ_MULT_DCD
        primary key (SERIAL_NUM, ID_JUSTIFIC, ID_TRANSACT, ID_PRODUCT)
);

