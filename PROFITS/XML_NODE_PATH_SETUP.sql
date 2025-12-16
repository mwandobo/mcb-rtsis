create table XML_NODE_PATH_SETUP
(
    ORDER0                     DECIMAL(15)  not null,
    HASH_KEY                   DECIMAL(15)  not null,
    HASH_SEQ_ID                INTEGER      not null,
    USES_HASH_SEQ              CHAR(1),
    HASHED_XPATH               VARCHAR(500),
    LOCAL_NAME                 CHAR(50),
    HAS_TEXT                   CHAR(1),
    IS_ARRAY                   CHAR(1),
    IS_FIRST_NODE              CHAR(1),
    IS_ATTRIBUTE               CHAR(1),
    ORDERED_POSITION           SMALLINT,
    DEPTH                      INTEGER,
    SCHEMA_TYPE                CHAR(100),
    XPATH_TEMPLATE             VARCHAR(500) not null,
    FK_SWIFTSETUP_MESSAGE_TYPE VARCHAR(20),
    FK_SWIFTSETUP_TAG          VARCHAR(10),
    FK_SWIFTSETUP_MESSAGE_SN   INTEGER,
    FK_SWIFTSETUP_MSG_CATEGORY CHAR(1),
    FK_SWIFTSETUP_SUBTAG_SN    INTEGER,
    PARENT_HASH                DECIMAL(15),
    PARENT_SEQ_ID              INTEGER,
    PARENT_XPATH               VARCHAR(500),
    NEXT_SIBLING_HASH          DECIMAL(15),
    NEXT_SIBLING_SEQ_ID        INTEGER,
    NEXT_SIBLING_XPATH         VARCHAR(500),
    DESCRIPTION                VARCHAR(500),
    FK_XMLNAMESPACE_DOC_TYPE   CHAR(20)     not null,
    FK_XMLNAMESPACE_URN        CHAR(128)    not null,
    FK_XMLNAMESPACE_PREFIX     CHAR(10)     not null,
    ADD_NS_ATTR                CHAR(1)      not null,
    constraint XMLNPSI1
        primary key (FK_XMLNAMESPACE_DOC_TYPE, FK_XMLNAMESPACE_URN, FK_XMLNAMESPACE_PREFIX, HASH_KEY, HASH_SEQ_ID)
);

comment on table XML_NODE_PATH_SETUP is 'This entity defines the structure of the XML files via set of the ordered XPATHS. It also provides a link to the  SWIFT_SETUP_DETAIL entries';

comment on column XML_NODE_PATH_SETUP.ORDER0 is 'The increased id of the all nodes. Used to simplify and speed up gathering of the output files, where the records will be picked up in the order of the id.';

comment on column XML_NODE_PATH_SETUP.HASH_KEY is 'A hash value calculated on the Xpath string. Expected to be unique.';

comment on column XML_NODE_PATH_SETUP.HASH_SEQ_ID is 'Hash sequentional id, used in case if the same Hash is generated for different XPATHs. The pair of the the Hash and the HASH_SEQ_ID is Unique.';

comment on column XML_NODE_PATH_SETUP.USES_HASH_SEQ is 'Indicates if for different XPATH more then 1 same hash has been calculated. In order to match and set correct entry, the XPATHS need to be compared and the HASH_SEQ_ID used.';

comment on column XML_NODE_PATH_SETUP.HASHED_XPATH is 'The text used for calculating the HASH_KEY. Consists from the Xpath with standard namespaces''s prefixes, that leads to the node. This XPATH doesn''t contain the interation indeces.';

comment on column XML_NODE_PATH_SETUP.LOCAL_NAME is 'The LOCAL_NAME of the node or of the attribute. Used to match entry in the XML_ELEMENT_SETUP table.';

comment on column XML_NODE_PATH_SETUP.HAS_TEXT is 'Y if the XPATH leads to the text node or to text attribute, otherwise it is N';

comment on column XML_NODE_PATH_SETUP.IS_ARRAY is 'Y if the XPATH leads to the arrays of nodes, otherwise it is N';

comment on column XML_NODE_PATH_SETUP.IS_FIRST_NODE is 'this field in used during creation of the output data designates the very first child node of the created document';

comment on column XML_NODE_PATH_SETUP.IS_ATTRIBUTE is 'this field in used during creation of the output data designates that the text will be rendered as an attribute';

comment on column XML_NODE_PATH_SETUP.ORDERED_POSITION is 'this field in used during creation of the output paths in correct order';

comment on column XML_NODE_PATH_SETUP.DEPTH is 'The depth of node''s path, i.e. how many nodes are passed until this node. Used to match entry in the XML_ELEMENT_SETUP table.';

comment on column XML_NODE_PATH_SETUP.SCHEMA_TYPE is 'The node type as defined in the schema. Used to match entry in the XML_ELEMENT_SETUP table.';

comment on column XML_NODE_PATH_SETUP.XPATH_TEMPLATE is 'The template of the node''s XPATH to be used (filled) during file output. It indicates the repeatative nodes by [*]';

comment on column XML_NODE_PATH_SETUP.FK_SWIFTSETUP_MESSAGE_TYPE is 'Part of the foreign key  to the SWIFT_SETUP_DETAIL';

comment on column XML_NODE_PATH_SETUP.FK_SWIFTSETUP_TAG is 'Part of the foreign key  to the SWIFT_SETUP_DETAIL';

comment on column XML_NODE_PATH_SETUP.FK_SWIFTSETUP_MESSAGE_SN is 'Part of the foreign key  to the SWIFT_SETUP_DETAIL';

comment on column XML_NODE_PATH_SETUP.FK_SWIFTSETUP_MSG_CATEGORY is 'Part of the foreign key  to the SWIFT_SETUP_DETAIL';

comment on column XML_NODE_PATH_SETUP.FK_SWIFTSETUP_SUBTAG_SN is 'Part of the foreign key to the SWIFT_SETUP_DETAIL';

comment on column XML_NODE_PATH_SETUP.PARENT_HASH is 'HASH_KEY callulated on the xpath of parent';

comment on column XML_NODE_PATH_SETUP.PARENT_SEQ_ID is 'Parent''s hash sequentional id, used in case if the same Hash is generated for differentParent XPATHs. The pair of the the PARENT_HASH and the PARENT_HASH_SEQ_ID is Unique.';

comment on column XML_NODE_PATH_SETUP.PARENT_XPATH is 'The text used for calculating the PARENT_HASH.';

comment on column XML_NODE_PATH_SETUP.NEXT_SIBLING_HASH is 'HASH_KEY callulated on the xpath of next sibling';

comment on column XML_NODE_PATH_SETUP.NEXT_SIBLING_SEQ_ID is 'NEXT_SIBLING''s hash sequentional id, used in case if the same Hash is generated for different NEXT_SIBLING XPATHs. The pair of the the NEXT_SIBLING_HASH and the NEXT_SIBLING_HASH_SEQ_ID is Unique.';

comment on column XML_NODE_PATH_SETUP.NEXT_SIBLING_XPATH is 'The text used for calculating the NEXT_SIBLING_HASH.';

create unique index IX_MSGTYPE_LCLNAME
    on XML_NODE_PATH_SETUP (FK_SWIFTSETUP_MESSAGE_TYPE, LOCAL_NAME);

create unique index XMLNPSPK
    on XML_NODE_PATH_SETUP (FK_XMLNAMESPACE_DOC_TYPE, FK_XMLNAMESPACE_URN, FK_XMLNAMESPACE_PREFIX, HASH_SEQ_ID, ORDER0);

