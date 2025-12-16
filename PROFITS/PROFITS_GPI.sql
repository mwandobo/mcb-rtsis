create table PROFITS_GPI
(
    BIC                 CHAR(11) not null
        constraint PK_GPI_DIRECTORY
            primary key,
    OUR_BANK_FLG        CHAR(1),
    NON_GPI_MEMBER      CHAR(1),
    GENERATE_121_FIELD  CHAR(1),
    REFERENCE_121_FIELD CHAR(1),
    CONFIRMATIONS       CHAR(1),
    REJECTIONS          CHAR(1),
    CONF_REJ_RECEIVER   CHAR(11),
    CONF_REJ_ACTIVE     DATE,
    OTHR_REJ_INACTIVE   DATE
);

