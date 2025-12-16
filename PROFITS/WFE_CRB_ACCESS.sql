create table WFE_CRB_ACCESS
(
    REQUEST_CUST_ID      INTEGER     not null,
    WFE_CRB_ACCESS_ID    DECIMAL(10) not null,
    APPLICATION_SYSTEM   SMALLINT    not null,
    APPLICATION_ID       CHAR(40)    not null,
    APPLICATION_PRODUCT  INTEGER,
    APPLICATION_AMN      DECIMAL(15, 2),
    APPLICATION_CURRENCY INTEGER,
    TRX_COMMENTS         VARCHAR(2048),
    FILE_SN_XML_IN       DECIMAL(15),
    FILE_SN_XML_OUT      DECIMAL(15),
    FILE_SN_DOCUMENT     DECIMAL(15),
    TRX_UNIT             INTEGER,
    TRX_DATE             DATE,
    TRX_USR              CHAR(8),
    TRX_TMSTAMP          TIMESTAMP(6),
    TRX_USER_SN          INTEGER,
    FK_CRB_GH            CHAR(5)     not null,
    FK_CRB_GD            INTEGER     not null,
    constraint PK_WFE_CRB
        primary key (REQUEST_CUST_ID, WFE_CRB_ACCESS_ID, FK_CRB_GH, FK_CRB_GD)
);

