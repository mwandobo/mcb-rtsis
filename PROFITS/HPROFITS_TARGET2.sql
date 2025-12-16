create table HPROFITS_TARGET2
(
    TMSTAMP             TIMESTAMP(6) not null,
    CNTR                DECIMAL(10)  not null,
    VALID_TILL          DATE,
    VALID_FROM          DATE,
    TYPE_OF_CHANGE      CHAR(1),
    BIC_FLAG            CHAR(1),
    TYPE_OF_PARTICIPANT CHAR(2),
    ADDRESSEE           CHAR(11),
    BIC                 CHAR(11),
    ACCOUNT_HOLDER      CHAR(11),
    SORTING_CODE        CHAR(15),
    CITY_HEADING        CHAR(35),
    INSTITUTION_NAME    CHAR(105),
    constraint IXU_FX_007
        primary key (TMSTAMP, CNTR)
);

