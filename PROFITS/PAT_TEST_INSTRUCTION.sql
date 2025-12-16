create table PAT_TEST_INSTRUCTION
(
    INSTRUCTION_ID      DECIMAL(10) not null,
    INDEX1              SMALLINT    not null,
    INTRUCTION_TYPE     SMALLINT    not null,
    MAXIMUM_WAIT_TIME   SMALLINT    not null,
    FK_PAT_ACTIONSTA_ID INTEGER     not null,
    FK_PAT_WINCONTRUID0 DECIMAL(10),
    ERROR_HANDLING_TYPE SMALLINT,
    STATUS              CHAR(1)     not null,
    LAST_CHANGED        TIMESTAMP(6),
    constraint PATINPK1
        primary key (FK_PAT_ACTIONSTA_ID, INSTRUCTION_ID)
);

create unique index PATINI1
    on PAT_TEST_INSTRUCTION (FK_PAT_WINCONTRUID0);

