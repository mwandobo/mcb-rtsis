create table EXCEPTION_ACCOUNT
(
    ACC_UNIT INTEGER     not null,
    ACC_TYPE SMALLINT    not null,
    ACC_SN   DECIMAL(15) not null,
    JUSTIFIC INTEGER,
    constraint PK_EXCEPTION_ACCOUNT
        primary key (ACC_SN, ACC_TYPE, ACC_UNIT)
);

