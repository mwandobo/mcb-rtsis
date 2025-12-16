create table PROFITS_GPI_DTL
(
    FK_GPI_BIC   CHAR(11) not null,
    RECORD_TYPE  CHAR(5)  not null,
    SN           INTEGER  not null,
    MSG_TYPE     CHAR(2),
    MESSAGE_TYPE CHAR(20),
    constraint PK_PROFITS_GPI_DTL
        primary key (FK_GPI_BIC, RECORD_TYPE, SN)
);

