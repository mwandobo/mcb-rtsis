create table DCD_ROUTINE
(
    PRFT_SYSTEM        SMALLINT    not null,
    ROUTINE_NAME       CHAR(80)    not null,
    ROUTINE_SN         DECIMAL(12) not null,
    SOURCE_NAME        CHAR(10),
    MODEL_ID           DECIMAL(12),
    FUNCTIONALITY_DESC VARCHAR(2048),
    constraint PKDCD04
        primary key (PRFT_SYSTEM, ROUTINE_NAME, ROUTINE_SN)
);

