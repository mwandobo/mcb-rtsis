create table PARTY_SEL_CHANNEL
(
    FK_CURRENCY_ID INTEGER not null,
    FK_CHANNEL_ID  INTEGER not null,
    FK_CUSTOMER_ID INTEGER not null,
    EXCEPT_CHANNEL CHAR(1) not null,
    constraint PK_PARTY_CHANNEL
        primary key (FK_CHANNEL_ID, FK_CURRENCY_ID, FK_CUSTOMER_ID)
);

