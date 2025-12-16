create table LNS_INSTANT_APP_REPRES
(
    APPLICATION_ID DECIMAL(18) not null,
    CUST_ID        INTEGER     not null,
    constraint PK_INSTANT_APP_REP
        primary key (CUST_ID, APPLICATION_ID)
);

comment on column LNS_INSTANT_APP_REPRES.CUST_ID is 'It is a unique customer identification number given automatically by the system.';

