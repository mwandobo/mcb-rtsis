create table DEP_ACCOUNT_OWNERS
(
    ACC_OWNER_SN       SMALLINT,
    FK_DEPOSIT_ACCOACC DECIMAL(11) not null,
    FK_CUSTOMERCUST_ID INTEGER     not null,
    constraint PK_ID
        primary key (FK_DEPOSIT_ACCOACC, FK_CUSTOMERCUST_ID)
);

