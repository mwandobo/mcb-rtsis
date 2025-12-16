create table DCD_TRNS_RULE_MTCH
(
    CODE              CHAR(8)      not null,
    INTERNAL_SN       INTEGER      not null,
    LINE_WHERE_CALLED INTEGER      not null,
    PRFT_SYSTEM       SMALLINT     not null,
    ROUTINE_SN        DECIMAL(12)  not null,
    ROUTINE_SYSTEM    SMALLINT     not null,
    TMPSTAMP          TIMESTAMP(6) not null,
    VALRULE_ID        DECIMAL(12)  not null,
    STATUS0           CHAR(1),
    INPUT_OUTPUT_FLG  CHAR(1),
    SEND_OUT_TO_CLT   CHAR(1),
    FIELD_TYPE        CHAR(2),
    PASSWORD          CHAR(26),
    ROUTINE_ALIAS     CHAR(40),
    ROUTINE_TABLE     CHAR(40),
    ROUTINE_ATTRIBUTE CHAR(40),
    RULE_TABLE        CHAR(40),
    RULE_ATTRIBUTE    CHAR(40),
    constraint IXU_DEF_076
        primary key (CODE, INTERNAL_SN, LINE_WHERE_CALLED, PRFT_SYSTEM, ROUTINE_SN, ROUTINE_SYSTEM, TMPSTAMP,
                     VALRULE_ID)
);

