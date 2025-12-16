create table DCD_TRNS_ROUT_DATA
(
    CODE              CHAR(8)      not null,
    INTERNAL_SN       DECIMAL(12)  not null,
    PRFT_SYSTEM       SMALLINT     not null,
    ROUTINE_SN        DECIMAL(12)  not null,
    TMPSTAMP          TIMESTAMP(6) not null,
    MODEL_ID          DECIMAL(12),
    STATUS0           CHAR(1),
    INPUT_OUTPUT_FLG  CHAR(1),
    FIELD_TYPE        CHAR(2),
    PASSWORD          CHAR(26),
    ROUTINE_TABLE     CHAR(40),
    ROUTINE_ALIAS     CHAR(40),
    ROUTINE_ATTRIBUTE CHAR(40),
    constraint IXU_DEF_071
        primary key (CODE, INTERNAL_SN, PRFT_SYSTEM, ROUTINE_SN, TMPSTAMP)
);

