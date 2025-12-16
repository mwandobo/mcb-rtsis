create table XPAT_ACTION
(
    TA_ID                       INTEGER  not null
        constraint PATXACPK
            primary key,
    PRFT_SYSTEM                 INTEGER  not null,
    CODE                        INTEGER,
    TAG                         CHAR(40),
    DEFAULT_ERROR_HANDLING_TYPE CHAR(3),
    TIME_INTERVAL               INTEGER,
    STATUS                      CHAR(1)  not null,
    SUBCODE                     SMALLINT not null,
    FK_PAT_ACTIONSTA_ID         INTEGER
);

comment on column XPAT_ACTION.TA_ID is 'Unique Id of the Test Action';

comment on column XPAT_ACTION.PRFT_SYSTEM is 'A descriptive parameter. Can be used in order to check if during the overall testing of the particular PROFITS the executed set of senario covers every relevant test case01Customers ()02Products ()03Deposits ()04Loans () 05General Ledger ( )06Parameters';

comment on column XPAT_ACTION.CODE is 'A descriptive parameter. Can be used in order to check if during the overall testing of the particular PROFITS the executed set of senario covers every relevant transaction';

comment on column XPAT_ACTION.TAG is 'Free text description';

comment on column XPAT_ACTION.DEFAULT_ERROR_HANDLING_TYPE is 'If during instruction execution an error occurs and if no particular error handling is set on instruction level, this error handling will be applied.';

comment on column XPAT_ACTION.TIME_INTERVAL is 'Time interval in seconds between two consequential instructions in the test case';

comment on column XPAT_ACTION.STATUS is 'Status of the Action, the possible statuses are defined in the list of the attribute values.  Status is affected by Version Control as well and will disallow running of all scenarios that contains this Action';

comment on column XPAT_ACTION.SUBCODE is 'User defined suncode, used for organization of the actions in to sets, for search etc';

create unique index PATXACI1
    on XPAT_ACTION (FK_PAT_ACTIONSTA_ID);

