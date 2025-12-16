create table IPS_EXP_MULTI_FILES
(
    CREDIT_DEBIT         CHAR(10)    not null,
    BIC_ADDRESS          VARCHAR(12) not null,
    ORDER_CURRENCY       INTEGER     not null,
    ORI_GROUP_MESSAGE_ID CHAR(35)    not null,
    constraint IPS_EXP_MULTI_FILES_PK
        primary key (ORI_GROUP_MESSAGE_ID, ORDER_CURRENCY, BIC_ADDRESS, CREDIT_DEBIT)
);

comment on column IPS_EXP_MULTI_FILES.BIC_ADDRESS is 'The financial institution that needs to be credited with the order amount. BIC Address';

