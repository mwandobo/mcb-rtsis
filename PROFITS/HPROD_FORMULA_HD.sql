create table HPROD_FORMULA_HD
(
    FORMULA_SN         DECIMAL(10)  not null,
    FORMULA_DESC       VARCHAR(500),
    RESULT_FIELD       CHAR(40)     not null,
    ROUNDING_DIGITS    SMALLINT,
    ENTRY_STATUS       CHAR(1),
    TMSTAMP            TIMESTAMP(6) not null,
    FK_HPRODUCTID      INTEGER      not null,
    FK_HPRDVALIDITY_DT DATE         not null,
    constraint PK_HPROD_FORMULA_HD
        primary key (FK_HPRODUCTID, FK_HPRDVALIDITY_DT, TMSTAMP, RESULT_FIELD, FORMULA_SN)
);

