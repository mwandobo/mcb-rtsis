create table XPAT_TRANSACTION
(
    ID             INTEGER  not null
        constraint PATXTXPK
            primary key,
    TXN_NAME       CHAR(20) not null,
    VARIATION      CHAR(40),
    TEST_CATEGORY  CHAR(10),
    RECORDING_TYPE SMALLINT,
    STATUS         CHAR(1)  not null,
    SUBSET_CODE    INTEGER
);

comment on column XPAT_TRANSACTION.TXN_NAME is 'Unique Id of the Test Scenario';

comment on column XPAT_TRANSACTION.VARIATION is 'A descriptive parameter. Can be used in order to describe details/variation of transaction';

comment on column XPAT_TRANSACTION.TEST_CATEGORY is 'Free text description';

comment on column XPAT_TRANSACTION.RECORDING_TYPE is 'It defines what have to be recorded during scenario execution.';

comment on column XPAT_TRANSACTION.STATUS is 'Status of the Scenario, the possible statuses are defined in the list of the attribute values.  Status is affected by Version Control as well and will disallow running of the whole scenario.';

comment on column XPAT_TRANSACTION.SUBSET_CODE is 'User defined numeric code used for organization  of Scenario into  sets organization';

