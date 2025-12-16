create table XPAT_GENVALUE_TYPE
(
    GVT_ID                  CHAR(5)       not null
        constraint PATXVTPK
            primary key,
    CREATION_TIME_INDICATOR CHAR(1)       not null,
    PROVIDING_SYSTEM        CHAR(5),
    DESCRIPTION             CHAR(240),
    SCRIPT_PATH             VARCHAR(2000) not null
);

comment on column XPAT_GENVALUE_TYPE.GVT_ID is 'Unique ID identifing the  Method of instruction value setup';

comment on column XPAT_GENVALUE_TYPE.CREATION_TIME_INDICATOR is 'Creation Time indicator defines on which stage the value is created:  r - to be calculated when the instruction is being executed,  b - will be prepared at the test beginning, just before execution, p - during setup (parameter or permanent value),';

comment on column XPAT_GENVALUE_TYPE.PROVIDING_SYSTEM is 'Defines which method will be used to create complex value. More info provided in the properties of the values.';

comment on column XPAT_GENVALUE_TYPE.DESCRIPTION is 'Free text description of particular method and source of instruction value definition The  DESCRIPTION will be copied in the instructions story.';

