create table SWIFT_STP_PTJ
(
    BIC         CHAR(12) not null
        constraint PK_SWTIPTJ
            primary key,
    ID_PRODUCT  INTEGER  not null,
    ID_JUSTIFIC INTEGER  not null
);

