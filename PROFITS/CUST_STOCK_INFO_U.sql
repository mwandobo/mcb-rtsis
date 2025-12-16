create table CUST_STOCK_INFO_U
(
    FK_CUSTOMERCUST_ID INTEGER      not null
        constraint IXU_CIU_037
            primary key,
    PTS_USER_SN        DECIMAL(10),
    ASHS_CODE          DECIMAL(12),
    INVESTOR_ABBR      CHAR(15),
    TMSTAMP            TIMESTAMP(6) not null,
    INVESTOR_CODE      INTEGER      not null,
    COMMUNICATION_PERS CHAR(60)     not null,
    MAIL_PERSON        CHAR(60)     not null,
    PTS_USER_CODE      CHAR(10)     not null,
    PTS_REGISTRY       CHAR(11)     not null,
    PTS_ACCOUNT        CHAR(11)     not null,
    PTS_ACCOUNT_CONFIR CHAR(1)      not null,
    PTS_REGISTRY_CONFI CHAR(1)      not null,
    ENTRY_STATUS       CHAR(1)      not null,
    SHAREHOLDER_IND    CHAR(1),
    NO_OF_SHARES       INTEGER
);

