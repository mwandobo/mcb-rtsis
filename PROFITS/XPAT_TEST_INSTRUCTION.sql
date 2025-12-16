create table XPAT_TEST_INSTRUCTION
(
    INSTRUCTION_ID      DECIMAL(10) not null
        constraint PATXINPK
            primary key,
    INDEX1              SMALLINT    not null,
    INSTRUCTION_TYPE    SMALLINT    not null,
    ERROR_HANDLING_TYPE SMALLINT,
    MAXIMUM_WAIT_TIME   SMALLINT    not null,
    STATUS              CHAR(1)     not null,
    LAST_CHANGED        TIMESTAMP(6),
    FK_XPAT_ACTIONTA_ID INTEGER,
    FK_XPAT_WINCONTUID  DECIMAL(10)
);

comment on column XPAT_TEST_INSTRUCTION.INSTRUCTION_ID is 'ID of the instruction that is unique within the given test case.';

comment on column XPAT_TEST_INSTRUCTION.INDEX1 is 'One-based index that defines in which order the instructions will be executed.';

comment on column XPAT_TEST_INSTRUCTION.INSTRUCTION_TYPE is 'Identifies what type of human action the instruction will simulate';

comment on column XPAT_TEST_INSTRUCTION.ERROR_HANDLING_TYPE is 'Defines how to handle special conditions occured during instruction execution, Particularly, it is important for Comparison/Verification Instructions. :-1 = Ignore this option (default)1 = Means that if values do match, then error is found, test case has';

comment on column XPAT_TEST_INSTRUCTION.LAST_CHANGED is 'The timestamp indicating when the Instruction had been changed via PROFITS Version control procedure.';

create unique index PATXINI1
    on XPAT_TEST_INSTRUCTION (FK_XPAT_ACTIONTA_ID);

create unique index PATXINI2
    on XPAT_TEST_INSTRUCTION (FK_XPAT_WINCONTUID);

