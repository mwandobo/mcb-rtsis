create table CIE_FAVORITE_ACC
(
    ID             INTEGER  not null,
    ACCOUNT_TYPE   CHAR(1)  not null,
    STATUS         CHAR(1),
    BANK_NAME      CHAR(40),
    NICKNAME       CHAR(16) not null,
    CIE_CUSTOMER   INTEGER  not null,
    ACCOUNT_NUMBER CHAR(40) not null,
    constraint PK_FAVORITE_ACC
        primary key (CIE_CUSTOMER, ID)
);

