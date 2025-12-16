create table TEMP_TRIPLETS
(
    PROD_ID   INTEGER not null,
    TRX_ID    INTEGER not null,
    JUST_ID   INTEGER not null,
    AC_RULE   INTEGER not null,
    SN        DECIMAL(10),
    TIMESTMP  TIMESTAMP(6),
    USER_CODE CHAR(8) not null,
    constraint IXU_TEM_023
        primary key (PROD_ID, TRX_ID, JUST_ID, AC_RULE)
);

