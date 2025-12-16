create table XPAT_LOOP_VALUE
(
    ID                INTEGER  not null
        constraint PATXLVPK
            primary key,
    NAME0             CHAR(32),
    STATIC_DATA       CHAR(100),
    INDEX1            SMALLINT not null,
    FK_XPAT_USER_PRID CHAR(10),
    FK_XPAT_RUN_SCEID INTEGER
);

comment on column XPAT_LOOP_VALUE.ID is 'Unique id';

comment on column XPAT_LOOP_VALUE.NAME0 is 'The variable name, which static_data value will be replaced by the given loop value during future loop execution.  The name can be set to spaces. in such case the static value is a short comment to loop iteraction values set';

comment on column XPAT_LOOP_VALUE.STATIC_DATA is 'If the Name is not equal to spaces, then Static Data contains the variable''s value to be used during the test. Otherwise, the Static Data contains the description of the Loop''s step.';

comment on column XPAT_LOOP_VALUE.INDEX1 is 'loop index';

create unique index PATXLVI1
    on XPAT_LOOP_VALUE (FK_XPAT_USER_PRID);

create unique index PATXLVI2
    on XPAT_LOOP_VALUE (FK_XPAT_RUN_SCEID);

