create table GLOBAL_ROUTE_AMOUNT
(
    CORRESPONDENT_ID INTEGER not null,
    LIMIT_DATE       DATE    not null,
    LIMIT_CURRENCY   INTEGER not null,
    LIMIT_AMOUNT     DECIMAL(18, 2),
    constraint PK_GLOBAL_RL
        primary key (LIMIT_CURRENCY, LIMIT_DATE, CORRESPONDENT_ID)
);

comment on table GLOBAL_ROUTE_AMOUNT is 'GLOBAL_ROUTE_AMNSThe limit per Day is stored.It can be used for Limits calculation for xxx Days and/or Months.';

comment on column GLOBAL_ROUTE_AMOUNT.CORRESPONDENT_ID is 'CORRESPONDENT_IDThe associated Customer Correspondent associated with the routing.';

comment on column GLOBAL_ROUTE_AMOUNT.LIMIT_DATE is 'LIMIT_DATEThe date the limit data was created.';

comment on column GLOBAL_ROUTE_AMOUNT.LIMIT_CURRENCY is 'LIMIT_CURRENCYThe currency of the limit.Initially based on the routing rules - the limit is always in the domestic currency.';

comment on column GLOBAL_ROUTE_AMOUNT.LIMIT_AMOUNT is 'LIMIT_AMOUNTThe amount of the limit which has been used.';

