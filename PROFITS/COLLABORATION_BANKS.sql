create table COLLABORATION_BANKS
(
    BANK_ID       INTEGER     not null
        constraint P_COLLBAN
            primary key,
    SWIFT_ADDRESS VARCHAR(40) not null,
    BANK_NAME     VARCHAR(40) not null
);

