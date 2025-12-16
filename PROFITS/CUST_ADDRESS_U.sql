create table CUST_ADDRESS_U
(
    FKGD_HAS_AS_DISTRI    INTEGER,
    FKGH_HAS_AS_DISTRI    CHAR(5),
    FKGD_HAS_COUNTRY      INTEGER,
    FKGH_HAS_COUNTRY      CHAR(5),
    FK_CUSTOMERCUST_ID    INTEGER      not null,
    LATIN_IND             CHAR(1),
    MAIL_BOX              CHAR(5),
    SERIAL_NUM            SMALLINT     not null,
    ADDRESS_TYPE          CHAR(1)      not null,
    PTS_IND               CHAR(1),
    FAX_NO                CHAR(15),
    CITY                  CHAR(30),
    ZIP_CODE              CHAR(10),
    TELEPHONE             CHAR(15),
    COMMUNICATION_ADDRESS CHAR(1),
    ENTRY_STATUS          CHAR(1)      not null,
    TMSTAMP               TIMESTAMP(6) not null,
    REGION                VARCHAR(20),
    ADDRESS_1             VARCHAR(40),
    ADDRESS_2             VARCHAR(40),
    ENTRY_COMMENTS        VARCHAR(250),
    SEGM_FLAGS            CHAR(5),
    constraint IXU_CIU_020
        primary key (SERIAL_NUM, FK_CUSTOMERCUST_ID)
);

