create table TRS_DEAL_FEED
(
    KEY_WS_CODE        VARCHAR(20)  not null,
    KEY_REF_NUMBER     CHAR(100)    not null,
    TMSTAMP            TIMESTAMP(6) not null,
    TRX_DATE           DATE,
    TRX_UNIT           INTEGER,
    TRX_USR            CHAR(8),
    TRX_USR_SN         INTEGER,
    INTERNAL_SN        SMALLINT,
    TICKET_SN          INTEGER      not null,
    TICKET_DATE        DATE         not null,
    ACTION_DESCRIPTION VARCHAR(80),
    constraint PK_DEAL_FEED
        primary key (KEY_WS_CODE, KEY_REF_NUMBER)
);

