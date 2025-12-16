create table TEMP_DEPOS3
(
    DEPOSIT_TYPE    CHAR(1)  not null,
    ID_PRODUCT      INTEGER  not null,
    FREQUENCY       SMALLINT not null,
    NET_INTERESTS   DECIMAL(15, 2),
    INTERESTS       DECIMAL(15, 2),
    TAXES           DECIMAL(15, 2),
    ID_PRODUCT_DESC CHAR(40),
    constraint IXU_REP_180
        primary key (DEPOSIT_TYPE, ID_PRODUCT, FREQUENCY)
);

