create table SWIFT_ALLNCE_CONST
(
    BIC               CHAR(11) not null,
    PARAMETER_TYPE    CHAR(5)  not null,
    SERIAL_NUM        INTEGER  not null,
    SHORT_DESCRIPTION CHAR(10),
    DESCRIPTION       VARCHAR(40),
    LATIN_DESCRIPTION VARCHAR(40),
    TMSTAMP           TIMESTAMP(6),
    constraint PK_SWIFT_BIC_ATTR
        primary key (BIC, PARAMETER_TYPE, SERIAL_NUM)
);

