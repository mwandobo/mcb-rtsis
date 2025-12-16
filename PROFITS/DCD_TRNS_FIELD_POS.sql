create table DCD_TRNS_FIELD_POS
(
    TMPSTAMP           TIMESTAMP(6) not null,
    CODE               CHAR(8)      not null,
    INTERNAL_SN        INTEGER      not null,
    RULE_ID            DECIMAL(12)  not null,
    PRFT_SYSTEM        SMALLINT     not null,
    FIELD_GRP_POSITION SMALLINT,
    STATUS             CHAR(1),
    FIELD_TYPE         CHAR(2),
    TABLE_ENTITY       CHAR(40),
    TABLE_ATTRIBUTE    CHAR(40),
    TABLE_ALIAS        CHAR(40),
    constraint IXU_DEF_065
        primary key (TMPSTAMP, CODE, INTERNAL_SN, RULE_ID, PRFT_SYSTEM)
);

