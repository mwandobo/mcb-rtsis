create table CMS_LIMIT_PCODES
(
    SN              DECIMAL(10) not null,
    PROCESSING_CODE CHAR(10),
    ENTRY_STATUS    CHAR(1),
    FK_CMS_LIMIT    CHAR(15)    not null,
    constraint PK_LIMIT_PCODES
        primary key (FK_CMS_LIMIT, SN)
);

