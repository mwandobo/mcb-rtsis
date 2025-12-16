create table GWGKAT
(
    INSTITUTSNR VARCHAR(4)  not null,
    KATNR       DECIMAL(10) not null,
    ORGEINHEIT  DECIMAL(10) not null,
    PKL_KUERZEL CHAR(10)    not null,
    GUELTAB     CHAR(8)     not null,
    GUELTBIS    CHAR(8)     not null,
    constraint PK_DUMMY1
        primary key (KATNR, INSTITUTSNR)
);

