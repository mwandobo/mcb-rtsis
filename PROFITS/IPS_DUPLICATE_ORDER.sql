create table IPS_DUPLICATE_ORDER
(
    TIMESTAMP_CREATED TIMESTAMP(6) not null,
    ID                DECIMAL(15)  not null,
    FILE_NAME         CHAR(250),
    ORDER_CODE        VARCHAR(20),
    constraint IPSDORDPK
        primary key (TIMESTAMP_CREATED, ID)
);

