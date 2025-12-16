create table OTHER_AFM_U
(
    FK_CUSTOMERCUST_ID INTEGER      not null,
    FK_TAX_OFFICEID    SMALLINT,
    SERIAL_NO          SMALLINT     not null,
    AFM_NO             CHAR(20)     not null,
    ISSUE_DATE         DATE,
    EXPIRY_DATE        DATE,
    FK_ISSUECNTRY_SER  INTEGER,
    FK_ISSUECNTRY_DFK  CHAR(5),
    MAIN_FLAG          CHAR(1)      not null,
    TMSTAMP            TIMESTAMP(6) not null,
    constraint IXU_CIU_048
        primary key (SERIAL_NO, FK_CUSTOMERCUST_ID)
);

