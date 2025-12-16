create table CGN_VALUES
(
    TABLE_SN    DECIMAL(10) not null,
    FIELD_SN    DECIMAL(10) not null,
    VALUE_SN    INTEGER     not null,
    VALUE_CHAR  VARCHAR(20),
    VALUE_NUM   DECIMAL(15),
    VALUE_ALIAS VARCHAR(30),
    constraint PK_CGN_VALUES
        primary key (VALUE_SN, FIELD_SN, TABLE_SN)
);

