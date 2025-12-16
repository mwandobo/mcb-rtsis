create table TF_DESTINATION
(
    FK_LC_ACCOUNT_NUM CHAR(40) not null,
    SN                SMALLINT not null,
    RECEIVING_DATE    DATE,
    DESTINATION_DESCR CHAR(40),
    constraint IXU_FX_045
        primary key (FK_LC_ACCOUNT_NUM, SN)
);

