create table DCD_GRP_FIELD_DATA
(
    INTERNAL_SN     SMALLINT    not null,
    POSTING_ID      DECIMAL(12) not null,
    PRFT_SYSTEM     SMALLINT    not null,
    FIELD_TYPE      CHAR(2),
    DBASE_TABLE     CHAR(40),
    DBASE_ATTRIBUTE CHAR(40),
    constraint IXU_DEF_051
        primary key (INTERNAL_SN, POSTING_ID, PRFT_SYSTEM)
);

