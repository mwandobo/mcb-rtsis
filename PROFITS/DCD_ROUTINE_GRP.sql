create table DCD_ROUTINE_GRP
(
    PRFT_SYSTEM      SMALLINT    not null,
    ROUTINE_SN       DECIMAL(12) not null,
    ROUTINE_NAME     CHAR(80)    not null,
    GROUP_ID         INTEGER     not null,
    AT_MOST          DECIMAL(10),
    ON_AVERAGE       DECIMAL(10),
    AT_LEAST         DECIMAL(10),
    MODEL_ID         DECIMAL(12),
    INPUT_OUTPUT_FLG CHAR(1),
    IS_USED          CHAR(1),
    INPUT_OUTPUT     CHAR(1),
    AT_LEAST_IND     CHAR(2),
    INDEXED          CHAR(2),
    OCCURS           CHAR(2),
    AT_MOST_IND      CHAR(2),
    GROUP_NAME       CHAR(50),
    constraint IXU_DEF_118
        primary key (PRFT_SYSTEM, ROUTINE_SN, ROUTINE_NAME, GROUP_ID)
);

