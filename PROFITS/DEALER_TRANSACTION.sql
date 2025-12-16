create table DEALER_TRANSACTION
(
    ID_TRANSACT INTEGER not null
        constraint PK_DEALER_TRANSACTION
            primary key,
    DESCRIPTION VARCHAR(40)
);

comment on column DEALER_TRANSACTION.ID_TRANSACT is 'It is a unique number that identifies a specificbusiness transaction.*****************Comments****************** TR Code';

comment on column DEALER_TRANSACTION.DESCRIPTION is 'It is the description of a transaction.*****************Comments****************** TRDescrption';

