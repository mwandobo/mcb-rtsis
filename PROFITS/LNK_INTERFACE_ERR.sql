create table LNK_INTERFACE_ERR
(
    USERCODE       CHAR(8)  not null,
    UNITCODE       INTEGER  not null,
    TRN_DATE       DATE     not null,
    TRN_SNUM       INTEGER  not null,
    LINE_NUM       SMALLINT not null,
    GLG_ACCOUNT_ID CHAR(21) not null,
    SUBSYSTEM      CHAR(2),
    REMARKS        VARCHAR(40),
    constraint IXU_LNK_001
        primary key (USERCODE, UNITCODE, TRN_DATE, TRN_SNUM, LINE_NUM, GLG_ACCOUNT_ID)
);

