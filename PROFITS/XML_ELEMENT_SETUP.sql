create table XML_ELEMENT_SETUP
(
    IS_ATTRIBUTE             CHAR(1)   not null,
    IS_LEAF                  CHAR(1),
    LOCAL_NAME               CHAR(50)  not null,
    DEPTH                    INTEGER   not null,
    VALUE_TYPE               CHAR(20),
    SYNTAX_SAMPLE            CHAR(50),
    SCHEMA_TYPE              CHAR(100) not null,
    REGEX_TO_MATCH           CHAR(100),
    MAX_LENGTH               INTEGER,
    ALLOWED_ENUM_LIST        VARCHAR(1000),
    FK_XMLNAMESPACE_DOC_TYPE CHAR(20)  not null,
    FK_XMLNAMESPACE_URN      CHAR(128) not null,
    FK_XMLNAMESPACE_PREFIX   CHAR(10)  not null,
    constraint XMLELSPK
        primary key (FK_XMLNAMESPACE_DOC_TYPE, FK_XMLNAMESPACE_URN, FK_XMLNAMESPACE_PREFIX, SCHEMA_TYPE, DEPTH,
                     LOCAL_NAME, IS_ATTRIBUTE)
);

comment on table XML_ELEMENT_SETUP is 'Contains the details of the XML Elements ( nodes and attributes), such as restrictions and is used for reference.';

comment on column XML_ELEMENT_SETUP.IS_LEAF is 'Defines if the given element bears text value';

comment on column XML_ELEMENT_SETUP.LOCAL_NAME is 'The LOCAL_NAME of the node or of the attribute. Used to match entry in the XML_NODE_PATH_SETUP table.';

comment on column XML_ELEMENT_SETUP.DEPTH is 'The depth of node''s path, i.e. how many nodes are passed until this node reached. Used to match entry in the XML_NODE_PATH_SETUP table.';

comment on column XML_ELEMENT_SETUP.VALUE_TYPE is 'Type of the value';

comment on column XML_ELEMENT_SETUP.SYNTAX_SAMPLE is 'An example of the value. it is automatically generated, sintaxically correct, but meaningless';

comment on column XML_ELEMENT_SETUP.SCHEMA_TYPE is 'The node type as defined in the schema. Used to match entry in the XML_NODE_PATH_SETUP table.';

comment on column XML_ELEMENT_SETUP.REGEX_TO_MATCH is 'Regex restriction';

comment on column XML_ELEMENT_SETUP.MAX_LENGTH is 'The maximum allowed length of the value';

comment on column XML_ELEMENT_SETUP.ALLOWED_ENUM_LIST is 'List of the fixed allowed values, separated by |';

