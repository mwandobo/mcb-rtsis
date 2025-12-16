create table CLASS_GL_TD_ORIGIN
(
    PRFT_SYSTEM    INTEGER                  not null,
    ORIGIN_ID      CHAR(2)                  not null,
    SQL_STATEMENT  VARCHAR(3000)            not null,
    SNUM           DECIMAL(15, 2) default 0 not null,
    TABLE_NAME     VARCHAR(30),
    TO_BE_EXCLUDED VARCHAR(1)     default ' ',
    PROGRAM_ID1    VARCHAR(5),
    PROGRAM_ID2    VARCHAR(5),
    PROGRAM_ID3    VARCHAR(5),
    PROGRAM_ID4    VARCHAR(5),
    PROGRAM_ID5    VARCHAR(5),
    constraint PK_CLASSGLORIGIN1
        primary key (PRFT_SYSTEM, ORIGIN_ID, SNUM)
);

