create table DCD_ROUTINE_GRP_SN
(
    PRFT_SYSTEM     SMALLINT    not null,
    ROUTINE_SN      DECIMAL(12) not null,
    ROUTINE_NAME    CHAR(80)    not null,
    GROUP_ID        INTEGER     not null,
    VIEW_SN         DECIMAL(10) not null,
    MODEL_ID        DECIMAL(12),
    FIELD_TYPE      CHAR(2),
    GROUP_ALIAS     CHAR(40),
    GROUP_TABLE     CHAR(40),
    GROUP_ATTRIBUTE CHAR(40),
    constraint IXU_DEF_009
        primary key (PRFT_SYSTEM, ROUTINE_SN, ROUTINE_NAME, GROUP_ID, VIEW_SN)
);

