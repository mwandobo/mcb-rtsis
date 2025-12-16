create table DCD_TRNS_RL_TS_OUT
(
    CODE              CHAR(8)      not null,
    INTERNAL_SN       DECIMAL(10)  not null,
    PRFT_SYSTEM       SMALLINT     not null,
    SN                DECIMAL(10)  not null,
    TMPSTAMP          TIMESTAMP(6) not null,
    VALRULE_ID        DECIMAL(12)  not null,
    EXIT_STATE_ID     DECIMAL(12),
    OUTPUT_NUMERIC_15 DECIMAL(15, 4),
    OUTPUT_TIMESTAMP  TIMESTAMP(6),
    CREATED_STAMP     TIMESTAMP(6),
    OUTPUT_DATE       DATE,
    STATUS0           CHAR(1),
    OUTPUT_FLAG_2     CHAR(2),
    FIELD_TYPE        CHAR(2),
    PASSWORD          CHAR(26),
    TCASE_TABLE       CHAR(40),
    TCASE_ATTRIBUTE   CHAR(40),
    OUTPUT_TEXT       VARCHAR(100),
    OUTPUT_TIME       TIME,
    constraint IXU_DEF_070
        primary key (CODE, INTERNAL_SN, PRFT_SYSTEM, SN, TMPSTAMP, VALRULE_ID)
);

