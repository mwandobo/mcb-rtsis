create table PROFITS_TARGET2
(
    BIC                 CHAR(11) not null
        constraint IXU_FX_023
            primary key,
    VALID_FROM          DATE,
    VALID_TILL          DATE,
    TMSTAMP             TIMESTAMP(6),
    BIC_FLAG            CHAR(1),
    DIRECT_MEMBER       CHAR(1),
    TYPE_OF_CHANGE      CHAR(1),
    TYPE_OF_PARTICIPANT CHAR(2),
    ADDRESSEE           CHAR(11),
    ACCOUNT_HOLDER      CHAR(11),
    SORTING_CODE        CHAR(15),
    CITY_HEADING        CHAR(35),
    INSTITUTION_NAME    CHAR(105)
);

