create table DCD_VOUCHER_FRM
(
    FORMAT_NAME CHAR(30) not null
        constraint PKDCDVC0
            primary key,
    DESCRIPTION CHAR(80),
    VAR_TYPE    SMALLINT
);

