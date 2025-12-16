create table CHEQUE_IMAGE
(
    IMAGE_ID          DECIMAL(12) not null
        constraint PK_CHEQUEIMG
            primary key,
    BANK_ID           INTEGER     not null,
    CHEQUE_ACC_NUMBER CHAR(23)    not null,
    CHEQUE_NUMBER     VARCHAR(20) not null,
    ISSUE_DATE        DATE        not null,
    STORE_STATUS      CHAR(1)     not null
);

