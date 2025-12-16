create table XML_NAMESPACE_SETUP
(
    ID                    INTEGER   not null,
    PREFIX                CHAR(10)  not null,
    URN                   CHAR(128) not null,
    DOCUMENT_TYPE         CHAR(20)  not null,
    DEFINES_ROOT_NODE     CHAR(1),
    DESCRIPTION           CHAR(250),
    AREA                  CHAR(20),
    SCHEMA_FOR_VALIDATION BLOB(4194304),
    FILE_LOCATION         VARCHAR(250),
    USE_HASH_SEQ_ID       CHAR(1),
    ENTRY_STATUS          CHAR(1)   not null,
    constraint XMLNSSPK
        primary key (DOCUMENT_TYPE, URN, PREFIX)
);

comment on table XML_NAMESPACE_SETUP is 'This entity contails the information about the namespaces and schemas used to validate the input and output XMLs.';

comment on column XML_NAMESPACE_SETUP.ID is 'Uninue ID, a number from an ascending sequence of integers';

comment on column XML_NAMESPACE_SETUP.ENTRY_STATUS is 'Value 0 in entry_status indicates whether the namespace is the valid (current) one';

create unique index XMLNSSI1
    on XML_NAMESPACE_SETUP (ID);

