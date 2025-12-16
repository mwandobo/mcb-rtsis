create table BUSINESS_PROC
(
    ID         DECIMAL(10) not null
        constraint ID1
            primary key,
    NAME       CHAR(32)    not null,
    SEQ        DECIMAL(10) not null,
    FK_MODELID DECIMAL(10)
);

create unique index I0000539
    on BUSINESS_PROC (FK_MODELID);

