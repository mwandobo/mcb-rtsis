create table ISS_HOUSING_THEFT
(
    TP_SO_IDENTIFIER DECIMAL(10) not null,
    THIEF_CODE       INTEGER     not null,
    constraint ISSHOUS0
        primary key (THIEF_CODE, TP_SO_IDENTIFIER)
);

