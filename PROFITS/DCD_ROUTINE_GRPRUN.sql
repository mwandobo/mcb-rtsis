create table DCD_ROUTINE_GRPRUN
(
    PRFT_SYSTEM       SMALLINT    not null,
    ROUTINE_SN        DECIMAL(12) not null,
    ROUTINE_TEST_SN   DECIMAL(12) not null,
    INTERNAL_SN       INTEGER     not null,
    GROUP_ID          INTEGER,
    GROUP_CARDINALITY DECIMAL(10),
    DATA_NUMBER_18_4  DECIMAL(18, 4),
    DATA_DATE         DATE,
    DATA_TMSTAMP      TIMESTAMP(6),
    INPUT_OUTPUT_FLG  CHAR(1),
    FIELD_TYPE        CHAR(2),
    DATA_FLAG_2       CHAR(2),
    GROUP_TABLE       CHAR(40),
    GROUP_ALIAS       CHAR(40),
    GROUP_ATTRIBUTE   CHAR(40),
    GROUP_NAME        CHAR(50),
    DATA_TEXT         VARCHAR(100),
    DATA_TIME         TIME,
    constraint IXU_DEF_055
        primary key (PRFT_SYSTEM, ROUTINE_SN, ROUTINE_TEST_SN, INTERNAL_SN)
);

