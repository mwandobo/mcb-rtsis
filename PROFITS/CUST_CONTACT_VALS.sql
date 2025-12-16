create table CUST_CONTACT_VALS
(
    CUST_ID           INTEGER     not null,
    CONTACT_SN        DECIMAL(10) not null,
    INTERNAL_SN       INTEGER     not null,
    GD_PARAMETER_TYPE CHAR(5),
    GD_SERIAL_NUM     INTEGER     not null,
    DESCRIPTION       VARCHAR(40),
    VALIDATED         CHAR(1),
    TMSTAMP           TIMESTAMP(6),
    constraint PK_CONTCHECKS
        primary key (INTERNAL_SN, CONTACT_SN, CUST_ID)
);

