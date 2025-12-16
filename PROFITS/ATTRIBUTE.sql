create table ATTRIBUTE
(
    ID               DECIMAL(10) not null
        constraint ID
            primary key,
    NAME             CHAR(32)    not null,
    DSD_NAME         CHAR(32)    not null,
    SEQ              DECIMAL(10) not null,
    OPT              CHAR(1)     not null,
    TYPE0            CHAR(1)     not null,
    DOMAIN           CHAR(1)     not null,
    VARYING_LENGTH   CHAR(1)     not null,
    LENGTH           INTEGER     not null,
    DEC_PLACES       INTEGER     not null,
    CASE_SENSITIVE   CHAR(1),
    PARENT_ENTITY_ID DECIMAL(10) not null,
    ORG_ID           DECIMAL(10) not null,
    ORG_ENCY_ID      DECIMAL(10) not null,
    FK_MODELID       DECIMAL(10)
);

create unique index I0000525
    on ATTRIBUTE (FK_MODELID);

