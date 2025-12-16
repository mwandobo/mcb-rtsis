create table SQL_LIST
(
    TIMESTMP TIMESTAMP(6) not null,
    INTSN    DECIMAL(10)  not null,
    VALUE    VARCHAR(80)  not null
);

create unique index PKSQLLST
    on SQL_LIST (INTSN, TIMESTMP);

