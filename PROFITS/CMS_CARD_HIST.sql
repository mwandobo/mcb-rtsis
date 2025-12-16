create table CMS_CARD_HIST
(
    TMSTAMP              TIMESTAMP(6) not null
        constraint PK_CMS_CARD_HIST
            primary key,
    CARD_SN              DECIMAL(10)  not null,
    CARD_NUMBER          CHAR(20)     not null,
    CARD_EXPIRY_DATE     DATE         not null,
    ENTRY_STATUS         CHAR(2),
    EMAIL                CHAR(60),
    MOBILE               CHAR(20),
    CREATION_DATE        DATE,
    TUN_DATE             DATE,
    TUN_UNIT             INTEGER,
    TUN_USR              CHAR(8),
    TUN_USR_SN           INTEGER,
    TUN_USR_INT_SN       SMALLINT,
    CARD_SURNAME_LATIN   CHAR(80),
    CARD_NAME_LATIN      CHAR(80),
    PIN_ISSUANCE_FLG     CHAR(1),
    PIN_STATE_FLG        CHAR(1),
    PVKI                 CHAR(1),
    PVV                  CHAR(4),
    CARD_REISSUE_FLG     CHAR(1),
    PIN_REISSUE_FLG      CHAR(1),
    URGENT_REISSUE_FLG   CHAR(1),
    COMMENTS             VARCHAR(1000),
    FK_CUST_ID           INTEGER,
    FK_CUST_ADDR_CUST    INTEGER,
    FK_CUST_ADDR_SN      SMALLINT,
    FK_DELTYP_GENERIC_HD CHAR(5),
    FK_DELTYP_GENERIC_SN INTEGER,
    FK_CRDTYP_GENERIC_HD CHAR(5),
    FK_CRDTYP_GENERIC_SN INTEGER,
    CARD_MEMBER          INTEGER,
    CARD_APPL_SN         DECIMAL(10),
    FATHERNAME_LATIN     CHAR(80),
    CARD_COUNT           INTEGER      not null,
    FK_SRVTYP_GENERIC_HD CHAR(5),
    FK_SRVTYP_GENERIC_SN INTEGER,
    FK_CMS_LIMIT_HDCD    CHAR(15),
    FK_CNCTYP_GENERIC_HD CHAR(5),
    FK_CNCTYP_GENERIC_SN INTEGER,
    PRODUCTION_STATUS    CHAR(2),
    PROD_DATE            DATE,
    PROD_UNIT            INTEGER,
    PROD_USR             CHAR(8),
    PROD_USR_SN          INTEGER,
    PROD_USR_INT_SN      SMALLINT,
    PROD_TMSTAMP         TIMESTAMP(6)
);

create unique index I0001037
    on CMS_CARD_HIST (FK_CNCTYP_GENERIC_HD, FK_CNCTYP_GENERIC_SN);

create unique index I0001081
    on CMS_CARD_HIST (FK_CUST_ID);

create unique index I0001088
    on CMS_CARD_HIST (FK_CUST_ADDR_CUST, FK_CUST_ADDR_SN);

create unique index I0001090
    on CMS_CARD_HIST (FK_DELTYP_GENERIC_HD, FK_DELTYP_GENERIC_SN);

create unique index I0001092
    on CMS_CARD_HIST (FK_CRDTYP_GENERIC_HD, FK_CRDTYP_GENERIC_SN);

create unique index I0001104
    on CMS_CARD_HIST (FK_CMS_LIMIT_HDCD);

create unique index I0001116
    on CMS_CARD_HIST (FK_SRVTYP_GENERIC_HD, FK_SRVTYP_GENERIC_SN);

