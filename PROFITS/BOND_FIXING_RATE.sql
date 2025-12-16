create table BOND_FIXING_RATE
(
    FK_TRBONDBOND_CODE CHAR(15) not null,
    ACTIVATION_DATE    DATE     not null,
    REFERENCE_VALUE    DECIMAL(12, 6),
    RATE               DECIMAL(12, 6),
    TMSTAMP            TIMESTAMP(6),
    ACTIVATION_TIME    TIME,
    constraint IXU_DEP_115
        primary key (FK_TRBONDBOND_CODE, ACTIVATION_DATE)
);

