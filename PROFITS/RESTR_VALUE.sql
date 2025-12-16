create table RESTR_VALUE
(
    DESCRIPTION       CHAR(40),
    TYPE0             INTEGER not null,
    FK_PROD_RESTRICFK INTEGER not null,
    FK0PROD_RESTRICFK INTEGER not null,
    ENTRY_STATUS      CHAR(1),
    constraint PRESTRVA
        primary key (FK0PROD_RESTRICFK, FK_PROD_RESTRICFK, TYPE0)
);

