create table ASSET_REAL_ESTATE
(
    FK_ASSET_ID            VARCHAR(10) not null,
    FK_CUSTOMER_ID         DECIMAL(7)  not null,
    FK_ASSET_TYPE_CONTRACT DECIMAL(5)  not null,
    FK_ASSET_PAY_METHOD    DECIMAL(5)  not null,
    DATE_FROM              DATE        not null,
    DATE_TO                DATE        not null,
    RENT_UNIT              DECIMAL(3),
    RENT_FREQ_PAYMENT      DECIMAL(5),
    RENTAL_AMOUNT          DECIMAL(18, 2),
    FK_RENTAL_CURRENCY     DECIMAL(5),
    FK_PRFT_SYSTEM         DECIMAL(2),
    FK_ACCOUNT_NUMBER      CHAR(40),
    COMMENTS               VARCHAR(500),
    RENTS_IN_ADVANCE       DECIMAL(18, 2),
    constraint IXU_ASSET_REAL_ESTATE
        primary key (FK_ASSET_ID, DATE_FROM)
);

