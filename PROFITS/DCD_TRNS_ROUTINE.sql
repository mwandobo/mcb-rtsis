create table DCD_TRNS_ROUTINE
(
    CODE               CHAR(8)      not null,
    PRFT_SYSTEM        SMALLINT     not null,
    ROUTINE_NAME       CHAR(80)     not null,
    ROUTINE_SN         DECIMAL(12)  not null,
    TMPSTAMP           TIMESTAMP(6) not null,
    MODEL_ID           DECIMAL(12),
    STATUS0            CHAR(1),
    SOURCE_NAME        CHAR(10),
    PASSWORD           CHAR(26),
    FUNCTIONALITY_DESC VARCHAR(2048),
    constraint IXU_DEF_073
        primary key (CODE, PRFT_SYSTEM, ROUTINE_NAME, ROUTINE_SN, TMPSTAMP)
);

