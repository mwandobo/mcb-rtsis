create table ENTITYCRITERIA
(
    ATTRIBUTENAME CHAR(33) not null,
    TABLENAME     CHAR(40) not null,
    constraint PKENTITYCRITERIA
        primary key (TABLENAME, ATTRIBUTENAME)
);

