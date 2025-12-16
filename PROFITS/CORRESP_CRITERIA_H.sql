create table CORRESP_CRITERIA_H
(
    VALUE_DATE         DATE    not null,
    FK_GENERIC_DETAFK  CHAR(5) not null,
    FK_GENERIC_DETASER INTEGER not null,
    constraint PK_CORRESP_CRITERIA_H
        primary key (FK_GENERIC_DETAFK, FK_GENERIC_DETASER, VALUE_DATE)
);

