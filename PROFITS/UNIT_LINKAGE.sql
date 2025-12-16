create table UNIT_LINKAGE
(
    UNIT            INTEGER not null,
    VALIDITY_DATE   DATE    not null,
    SUPERVISOR_UNIT INTEGER,
    TIMESTMP        DATE,
    STATUS          CHAR(1),
    constraint IXU_DEF_140
        primary key (UNIT, VALIDITY_DATE)
);

