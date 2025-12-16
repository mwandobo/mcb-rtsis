create table CP_INSURANCE_INFO
(
    INSURANCE_TYPE    CHAR(1)     not null,
    LAST_UPDATE       TIMESTAMP(6),
    MIN_HOLDER_AGE    SMALLINT,
    MAX_HOLDER_AGE    SMALLINT,
    FK_CP_AGREEMENTCP DECIMAL(10) not null
        constraint CPINSURA
            primary key
);

