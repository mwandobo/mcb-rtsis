create table OTHER_ID
(
    FK_CUSTOMERCUST_ID INTEGER,
    SERIAL_NO          SMALLINT,
    FKGD_HAS_TYPE      INTEGER,
    FKGD_HAS_BEEN_ISSU INTEGER,
    EXPIRY_DATE        DATE,
    TMSTAMP            TIMESTAMP(6),
    ISSUE_DATE         DATE,
    MAIN_FLAG          CHAR(1),
    FKGH_HAS_BEEN_ISSU CHAR(5),
    FKGH_HAS_TYPE      CHAR(5),
    ID_NO              CHAR(20),
    ISSUE_AUTHORITY    CHAR(30),
    INCOMPLETE_U_COMNT CHAR(30)
);

create unique index IXU_OTH_004
    on OTHER_ID (FK_CUSTOMERCUST_ID, SERIAL_NO);

create unique index IX_ID_NO
    on OTHER_ID (ID_NO);

