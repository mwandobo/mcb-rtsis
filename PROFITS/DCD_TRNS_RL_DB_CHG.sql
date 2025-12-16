create table DCD_TRNS_RL_DB_CHG
(
    CODE              CHAR(8)      not null,
    INTERNAL_SN       INTEGER      not null,
    PRFT_SYSTEM       SMALLINT     not null,
    TMPSTAMP          TIMESTAMP(6) not null,
    UPDATE_FIELD_LINE INTEGER      not null,
    VALRULE_ID        DECIMAL(12)  not null,
    STATUS0           CHAR(1),
    FIELD_TYPE        CHAR(2),
    PASSWORD          CHAR(26),
    DBASE_ATTRIBUTE   CHAR(40),
    DBASE_TABLE       CHAR(40),
    constraint IXU_DEF_069
        primary key (CODE, INTERNAL_SN, PRFT_SYSTEM, TMPSTAMP, UPDATE_FIELD_LINE, VALRULE_ID)
);

