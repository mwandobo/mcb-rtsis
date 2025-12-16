create table DCD_RULE_DBASE_CHG
(
    INTERNAL_SN       INTEGER     not null,
    PRFT_SYSTEM       SMALLINT    not null,
    UPDATE_FIELD_LINE INTEGER     not null,
    VALRULE_ID        DECIMAL(12) not null,
    FIELD_TYPE        CHAR(2),
    DBASE_TABLE       CHAR(40),
    DBASE_ATTRIBUTE   CHAR(40),
    constraint IXU_DEF_012
        primary key (INTERNAL_SN, PRFT_SYSTEM, UPDATE_FIELD_LINE, VALRULE_ID)
);

