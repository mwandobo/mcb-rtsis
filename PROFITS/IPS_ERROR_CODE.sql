create table IPS_ERROR_CODE
(
    PROFITS_ERROR CHAR(80)    not null,
    ERROR_GROUP   VARCHAR(10) not null,
    TMSTAMP       TIMESTAMP(6),
    TRX_USER      CHAR(8),
    constraint PK_IPS_ERROR_CODE
        primary key (ERROR_GROUP, PROFITS_ERROR)
);

