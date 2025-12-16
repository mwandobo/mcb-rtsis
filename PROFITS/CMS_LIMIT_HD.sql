create table CMS_LIMIT_HD
(
    CODE         CHAR(15) not null
        constraint PK_CMS_LIMIT_HD
            primary key,
    DESCRIPTION  CHAR(80),
    DEFAULT_IND  CHAR(1),
    TMSTAMP      TIMESTAMP(6),
    LIMIT_HD_SN  INTEGER  not null,
    ENTRY_STATUS CHAR(1)
);

