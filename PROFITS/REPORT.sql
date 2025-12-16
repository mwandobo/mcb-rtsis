create table REPORT
(
    ID                      DECIMAL(10)  not null
        constraint I0000308
            primary key,
    FQNAME                  CHAR(254)    not null,
    RUNDATE                 TIMESTAMP(6) not null,
    FK_CHECK_MODELMODELCODE CHAR(10),
    TYPE0                   CHAR(6)      not null
);

create unique index I0000305
    on REPORT (FK_CHECK_MODELMODELCODE);

