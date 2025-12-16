create table CMS_LIMIT_DT
(
    LIMIT_DT_SN        DECIMAL(10) not null,
    LIMIT_AMNT         DECIMAL(15, 2),
    LIMIT_CURRENCY     INTEGER,
    FREQUENCY          SMALLINT,
    FK_LIMIT_HD_CD     CHAR(15)    not null,
    FK_LIMIT_CD        CHAR(15),
    FK_FREQ_GENERIC_HD CHAR(5),
    FK_FREQ_GENERIC_SN INTEGER,
    ENTRY_STATUS       CHAR(1),
    LIMIT_TRANSACTIONS INTEGER,
    constraint PK_CMS_LIMIT_DT
        primary key (FK_LIMIT_HD_CD, LIMIT_DT_SN)
);

create unique index I0001135
    on CMS_LIMIT_DT (FK_LIMIT_CD);

create unique index I0001141
    on CMS_LIMIT_DT (FK_FREQ_GENERIC_HD, FK_FREQ_GENERIC_SN);

