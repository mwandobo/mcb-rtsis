create table DCD_GRP_DBASE_INFO
(
    POSTING_ID       DECIMAL(12) not null,
    PRFT_SYSTEM      SMALLINT    not null,
    FIELD_TYPE       CHAR(2),
    DESCRIPTION      CHAR(50),
    FULL_DESCRIPTION VARCHAR(2048),
    constraint IXU_DEF_005
        primary key (POSTING_ID, PRFT_SYSTEM)
);

