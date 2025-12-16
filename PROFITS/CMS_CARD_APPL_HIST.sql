create table CMS_CARD_APPL_HIST
(
    TMSTAMP              TIMESTAMP(6) not null
        constraint PK_CARD_APPL_HIS
            primary key,
    APPLICATION_SN       DECIMAL(10),
    ENTRY_STATUS         CHAR(2),
    EMAIL                CHAR(60),
    MOBILE               CHAR(20),
    CREATION_DATE        DATE,
    FINALIZATION_DATE    DATE,
    TUN_DATE             DATE,
    TUN_UNIT             INTEGER,
    TUN_USR              CHAR(8),
    TUN_USR_SN           INTEGER,
    TUN_USR_INT_SN       SMALLINT,
    TUN_APRV_DATE        DATE,
    TUN_APRV_UNIT        INTEGER,
    TUN_APRV_USR         CHAR(8),
    TUN_APRV_USR_SN      INTEGER,
    TUN_APRV_USR_IN_SN   SMALLINT,
    CARD_SURNAME_LATIN   CHAR(80),
    CARD_NAME_LATIN      CHAR(80),
    CARD_SN              DECIMAL(10),
    PIN_ISSUANCE_FLG     CHAR(1),
    FK_CUST_ADDR_CUST    INTEGER,
    FK_CUST_ADDR_SN      SMALLINT,
    FK_CUST_ID           INTEGER,
    FK_DELTYP_GENERIC_HD CHAR(5),
    FK_DELTYP_GENERIC_SN INTEGER,
    FK_CRDTYP_GENERIC_HD CHAR(5),
    FK_CRDTYP_GENERIC_SN INTEGER,
    FATHERNAME_LATIN     CHAR(80),
    CARD_ATMBIN          CHAR(8),
    CARD_COUNT           DECIMAL(10),
    FK_CMS_LIMIT_HDCD    CHAR(15),
    COMMENTS             VARCHAR(1000),
    CARD_REISSUE_FLG     CHAR(1),
    EXPIRY_DATE          DATE,
    EXP_ACC_NUMBER       CHAR(40),
    EXP_ACC_CD           SMALLINT,
    EXP_ACC_PRFSYS       SMALLINT,
    EXP_TUN_DATE         DATE,
    EXP_TUN_UNIT         INTEGER,
    EXP_TUN_USR          CHAR(8),
    EXP_TUN_USR_SN       INTEGER,
    EXP_TUN_USR_INT_SN   SMALLINT,
    EXP_TMSTAMP          TIMESTAMP(6),
    PAN                  CHAR(19)
);

create unique index I0001068
    on CMS_CARD_APPL_HIST (FK_CUST_ADDR_CUST, FK_CUST_ADDR_SN);

create unique index I0001070
    on CMS_CARD_APPL_HIST (FK_CUST_ID);

create unique index I0001072
    on CMS_CARD_APPL_HIST (FK_CRDTYP_GENERIC_HD, FK_CRDTYP_GENERIC_SN);

create unique index I0001077
    on CMS_CARD_APPL_HIST (FK_DELTYP_GENERIC_HD, FK_DELTYP_GENERIC_SN);

create unique index I0001097
    on CMS_CARD_APPL_HIST (FK_CMS_LIMIT_HDCD);

