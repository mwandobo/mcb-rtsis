create table SWIFT_STP_PROCESSED
(
    SN      DECIMAL(11) not null
        constraint PK_SWTSTPRS
            primary key,
    TMSTAMP TIMESTAMP(6)
);

