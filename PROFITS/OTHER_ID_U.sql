create table OTHER_ID_U
(
    FK_CUSTOMERCUST_ID INTEGER      not null,
    FKGH_HAS_TYPE      CHAR(5),
    FKGD_HAS_TYPE      INTEGER,
    FKGH_HAS_BEEN_ISSU CHAR(5),
    FKGD_HAS_BEEN_ISSU INTEGER,
    ISSUE_AUTHORITY    CHAR(30),
    SERIAL_NO          SMALLINT     not null,
    ID_NO              CHAR(20)     not null,
    ISSUE_DATE         DATE,
    EXPIRY_DATE        DATE,
    MAIN_FLAG          CHAR(1)      not null,
    TMSTAMP            TIMESTAMP(6) not null,
    INCOMPLETE_U_COMNT CHAR(30),
    constraint IXU_CIU_049
        primary key (SERIAL_NO, FK_CUSTOMERCUST_ID)
);

