create table DOF_DETAIL
(
    ACCOUNT_NUMBER  CHAR(40)    not null,
    ACCOUNT_CD      SMALLINT,
    PRFT_SYSTEM     SMALLINT    not null,
    PRIORITY_SN     INTEGER     not null,
    AMOUNT          DECIMAL(15, 2),
    DEDUCTION_CODE  CHAR(3),
    ENTRY_STATUS    CHAR(1),
    FK_CP_AGREEMENT DECIMAL(10) not null,
    FK_CUSTOMER     DECIMAL(7)  not null,
    constraint I0010963
        primary key (FK_CUSTOMER, FK_CP_AGREEMENT, PRFT_SYSTEM, ACCOUNT_NUMBER)
);

