create table SWEEP_HISTORY
(
    START_DATE         DATE        not null,
    EXPIRY_DATE        DATE,
    FREQUENCY          SMALLINT,
    MAXIMUM_AMOUNT     DECIMAL(15, 2),
    AMN_ACCUMULATED    DECIMAL(15, 2),
    TIMESTMP           TIMESTAMP(6),
    FK_DEPOSIT_ACCOACC DECIMAL(11) not null,
    FK0DEPOSIT_ACCOACC DECIMAL(11) not null,
    FK_SWEEP_TYPE      CHAR(1)     not null,
    constraint PK_SWEEP_HISTORY
        primary key (FK_DEPOSIT_ACCOACC, FK0DEPOSIT_ACCOACC, FK_SWEEP_TYPE, START_DATE)
);

