create table DCD_ROUTINE_RUN
(
    INTERNAL_SN       INTEGER     not null,
    PRFT_SYSTEM       SMALLINT    not null,
    ROUTINE_SN        DECIMAL(12) not null,
    ROUTINE_TEST_SN   DECIMAL(12) not null,
    DATA_NUMBER_18_4  DECIMAL(18, 4),
    DATA_TMSTAMP      TIMESTAMP(6),
    DATA_DATE         DATE,
    INPUT_OUTPUT_FLG  CHAR(1),
    DATA_FLAG_2       CHAR(2),
    FIELD_TYPE        CHAR(2),
    ROUTINE_ALIAS     CHAR(40),
    ROUTINE_ATTRIBUTE CHAR(40),
    ROUTINE_TABLE     CHAR(40),
    DATA_TEXT         VARCHAR(100),
    DATA_TIME         TIME,
    constraint IXU_DEF_056
        primary key (INTERNAL_SN, PRFT_SYSTEM, ROUTINE_SN, ROUTINE_TEST_SN)
);

