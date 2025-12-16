create table GWGIKUKA
(
    INSTITUTSNR VARCHAR(4)  not null,
    KUNDNR      CHAR(16)    not null,
    KATNR       DECIMAL(10) not null,
    ORGEINHEIT  DECIMAL(10) not null,
    constraint PK_DUMMY2
        primary key (ORGEINHEIT, KATNR, KUNDNR, INSTITUTSNR)
);

