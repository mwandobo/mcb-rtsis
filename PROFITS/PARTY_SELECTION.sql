create table PARTY_SELECTION
(
    FK_CURRENCY_ID     INTEGER not null,
    FK_CUSTOMER_ID     INTEGER not null,
    PARTY_TYPE         CHAR(2) not null,
    FK_PROFITS_ACCOUNT CHAR(40),
    FK_PROFITS_ACCSYS  SMALLINT,
    FK_GLG_ACCOUNT     CHAR(21),
    constraint PK_PARTY_SELL
        primary key (FK_CURRENCY_ID, FK_CUSTOMER_ID)
);

