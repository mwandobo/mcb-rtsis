create table WFE_CHECKS_DTL
(
    WFE_CHECKS_DTL_ID     INTEGER     not null,
    FK_WFE_CHECKS_HDR     DECIMAL(10) not null,
    FK_APPLICATION_SYSTEM SMALLINT    not null,
    FK_APPLICATION_ID     CHAR(40)    not null,
    ADDITIONAL_NOTES      VARCHAR(2048),
    CREATE_UNIT           INTEGER,
    CREATE_DATE           DATE,
    CREATE_USR            CHAR(8),
    CREATE_TMSTAMP        TIMESTAMP(6),
    UPDATE_UNIT           INTEGER,
    UPDATE_DATE           DATE,
    UPDATE_USR            CHAR(8),
    UPDATE_TMSTAMP        TIMESTAMP(6),
    constraint PK_WFE_CHECKS_DTL
        primary key (WFE_CHECKS_DTL_ID, FK_WFE_CHECKS_HDR, FK_APPLICATION_SYSTEM, FK_APPLICATION_ID)
);

