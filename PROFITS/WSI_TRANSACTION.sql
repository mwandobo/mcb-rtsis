create table WSI_TRANSACTION
(
    WS_ID            VARCHAR(20) not null,
    WS_COMMAND       CHAR(80)    not null,
    PROFITS_SYSTEM   VARCHAR(80),
    WS_DESCRIPTION   VARCHAR(80),
    WS_ANALYSIS      LONG VARCHAR(32700),
    WS_STATUS        CHAR(1),
    FK_SEC_WINDOW    CHAR(8),
    FK_SEC_OPERATION CHAR(8),
    PRFT_SYSTEM      SMALLINT,
    constraint PK_WSI_1004
        primary key (WS_COMMAND, WS_ID)
);

