create table CMS_CARD
(
    CARD_SN                DECIMAL(10) not null
        constraint PK_CARD
            primary key,
    CARD_NUMBER            CHAR(20),
    CARD_MEMBER            INTEGER,
    CARD_COUNT             DECIMAL(13),
    CARD_EXPIRY_DATE       DATE        not null,
    ENTRY_STATUS           CHAR(2),
    EMAIL                  CHAR(60),
    MOBILE                 CHAR(20),
    CREATION_DATE          DATE,
    TUN_DATE               DATE,
    TUN_UNIT               INTEGER,
    TUN_USR                CHAR(8),
    TUN_USR_SN             INTEGER,
    TUN_USR_INT_SN         SMALLINT,
    CARD_SURNAME_LATIN     CHAR(80),
    CARD_NAME_LATIN        CHAR(80),
    PIN_ISSUANCE_FLG       CHAR(1),
    PIN_STATE_FLG          CHAR(1),
    PVKI                   CHAR(1),
    PVV                    CHAR(4),
    CARD_REISSUE_FLG       CHAR(1),
    PIN_REISSUE_FLG        CHAR(1),
    TMSTAMP                TIMESTAMP(6),
    COMMENTS               VARCHAR(1000),
    FK_CUST_ADDR_CUST      INTEGER,
    FK_CUST_ADDR_SN        SMALLINT,
    FK_CUST_ID             INTEGER,
    FK_DELTYP_GENERIC_HD   CHAR(5),
    FK_DELTYP_GENERIC_SN   INTEGER,
    FK_CRDTYP_GENERIC_HD   CHAR(5),
    FK_CRDTYP_GENERIC_SN   INTEGER,
    CARD_APPL_SN           DECIMAL(10),
    URGENT_REISSUE_FLG     CHAR(1),
    FATHERNAME_LATIN       CHAR(80),
    FK_TRNTYP_GENERIC_HD   CHAR(5),
    FK_TRNTYP_GENERIC_SN   INTEGER,
    FK_CMS_LIMIT_HDCD      CHAR(15),
    INCLUDE_IN_CAF_FLG     CHAR(1),
    FK_CNCTYP_GENERIC_HD   CHAR(5),
    FK_CNCTYP_GENERIC_SN   INTEGER,
    EXTRAIT_SN             DECIMAL(10),
    PRODUCTION_STATUS      CHAR(2),
    PROD_DATE              DATE,
    PROD_UNIT              INTEGER,
    PROD_USR               CHAR(8),
    PROD_USR_SN            INTEGER,
    PROD_USR_INT_SN        SMALLINT,
    PROD_TMSTAMP           TIMESTAMP(6),
    PRINT_REF_CODE         CHAR(40),
    FULL_CARD_NO           CHAR(40),
    CARD_TYPE_1            CHAR(1),
    SEQ_NO                 CHAR(3) default '000',
    EXPORT_ID              DECIMAL(10),
    REJECTION_REASON       CHAR(80),
    RECEIVED_TIMESTAMP     TIMESTAMP(6),
    CONFIRMATION_TIMESTAMP TIMESTAMP(6),
    INCLUDE_ON_CMS         CHAR(1),
    ANNIVERSARY_DT         DATE,
    OLD_FULL_CARD_NO       VARCHAR(40),
    "OLD_ENTRY_sTATUS"     CHAR(2)
);

create unique index ATM_ONL_TRAN_INDX
    on CMS_CARD (CARD_COUNT, CARD_NUMBER);

create unique index ATM_TRAN_INDX2
    on CMS_CARD (FULL_CARD_NO);

create unique index I0000996
    on CMS_CARD (FK_CNCTYP_GENERIC_HD, FK_CNCTYP_GENERIC_SN);

create unique index I0001045
    on CMS_CARD (FK_CUST_ADDR_CUST, FK_CUST_ADDR_SN);

create unique index I0001050
    on CMS_CARD (FK_CUST_ID);

create unique index I0001057
    on CMS_CARD (FK_DELTYP_GENERIC_HD, FK_DELTYP_GENERIC_SN);

create unique index I0001058
    on CMS_CARD (FK_CRDTYP_GENERIC_HD, FK_CRDTYP_GENERIC_SN);

create unique index I0001100
    on CMS_CARD (FK_CMS_LIMIT_HDCD);

create unique index I0001118
    on CMS_CARD (FK_TRNTYP_GENERIC_HD, FK_TRNTYP_GENERIC_SN);

