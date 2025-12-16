create table OTHER_AFM
(
    FK_CUSTOMERCUST_ID INTEGER,
    SERIAL_NO          SMALLINT,
    FK_TAX_OFFICEID    SMALLINT,
    FK_ISSUECNTRY_SER  INTEGER,
    EXPIRY_DATE        DATE,
    ISSUE_DATE         DATE,
    TMSTAMP            TIMESTAMP(6),
    MAIN_FLAG          CHAR(1),
    FK_ISSUECNTRY_DFK  CHAR(5),
    AFM_NO             CHAR(20)
);

create unique index IXU_OTH_001
    on OTHER_AFM (FK_CUSTOMERCUST_ID, SERIAL_NO);

create unique index IX_AFM_NO
    on OTHER_AFM (AFM_NO);

