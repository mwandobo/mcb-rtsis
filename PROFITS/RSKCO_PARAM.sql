create table RSKCO_PARAM
(
    PARAM_TYPE       CHAR(10) not null,
    SERIAL_NUM       INTEGER  not null,
    P_NUM            DECIMAL(15),
    P_DATE           DATE,
    LAST_INS_UP_DATE DATE,
    LAST_INS_UP_USER CHAR(8),
    REF_KEY          CHAR(20),
    P_LITERAL        CHAR(50),
    COMMENTS         CHAR(80),
    constraint IXU_LNS_056
        primary key (PARAM_TYPE, SERIAL_NUM)
);

