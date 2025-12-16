create table DCD_ROUTINE_DATA
(
    INTERNAL_SN       DECIMAL(12) not null,
    PRFT_SYSTEM       SMALLINT    not null,
    ROUTINE_SN        DECIMAL(12) not null,
    MODEL_ID          DECIMAL(12),
    INPUT_OUTPUT_FLG  CHAR(1),
    FIELD_TYPE        CHAR(2),
    ROUTINE_ATTRIBUTE CHAR(40),
    ROUTINE_ALIAS     CHAR(40),
    ROUTINE_TABLE     CHAR(40),
    constraint IXU_DEF_008
        primary key (INTERNAL_SN, PRFT_SYSTEM, ROUTINE_SN)
);

