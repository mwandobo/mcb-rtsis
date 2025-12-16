create table LC_AMENDMENT
(
    LC_ACCOUNT_NUMBER CHAR(41)    not null,
    SN                DECIMAL(10) not null,
    O_EXPIRY_DATE     DATE,
    N_EXPIRY_DATE     DATE,
    O_LC_AMOUNT       DECIMAL(15, 2),
    N_LC_AMOUNT       DECIMAL(15, 2),
    TMSTAMP           TIMESTAMP(6),
    constraint LC_AMENDMENT_PK
        primary key (SN, LC_ACCOUNT_NUMBER)
);

