create table CGN_FIELDS
(
    TABLE_SN          DECIMAL(10)  not null,
    FIELD_SN          DECIMAL(10)  not null,
    FIELD_TYPE        CHAR(1),
    VALUES_TYPE       CHAR(1)      not null,
    GD_PARAMETER_TYPE CHAR(5),
    FLD_FIELD_ALIAS   VARCHAR(40)  not null,
    FLD_FIELD         VARCHAR(400) not null,
    FLD_FIELD_APPEND  VARCHAR(5),
    constraint PK_CGN_FIELDS
        primary key (FIELD_SN, TABLE_SN)
);

