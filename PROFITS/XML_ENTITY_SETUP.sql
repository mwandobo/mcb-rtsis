create table XML_ENTITY_SETUP
(
    ID                       DECIMAL(10) not null
        constraint XMLENTPK
            primary key,
    ENTITY_GROUP             SMALLINT,
    DIMENTION_COUNT          SMALLINT,
    COMMON_HASHED_PREFIX     VARCHAR(500),
    XPATH_HEAD               CHAR(100)   not null,
    XPATH_PART1              CHAR(100),
    XPATH_PART2              CHAR(100),
    XPATH_PART3              CHAR(100),
    XPATH_PART4              CHAR(100),
    XPATH_PART5              CHAR(100),
    XPATH_PART6              CHAR(100),
    XPATH_TAIL_TEXTS         CHAR(100),
    XPATH_TAIL_ATTR          VARCHAR(100),
    ENTRY_STATUS             CHAR(1)     not null,
    AREA                     CHAR(20),
    DESCRIPTION              CHAR(250),
    FK_XMLNAMESPACE_DOC_TYPE CHAR(20),
    FK_XMLNAMESPACE_URN      CHAR(128),
    FK_XMLNAMESPACE_PREFIX   CHAR(10),
    FK_SWG_MESSAGE_TYPE      CHAR(20),
    RECORD_TYPE              SMALLINT
);

comment on table XML_ENTITY_SETUP is 'The entries in the tables are used to map the XML nodes to the PROFITS entities';

comment on column XML_ENTITY_SETUP.ID is 'User defined unique entity code';

comment on column XML_ENTITY_SETUP.ENTITY_GROUP is 'Defines how entities are grouped';

comment on column XML_ENTITY_SETUP.DIMENTION_COUNT is 'Indicates the dimentions of the node/entity''s array. The relevant indeces will be set in the XML_INPUT_FILE to point to the node/entity.';

comment on column XML_ENTITY_SETUP.COMMON_HASHED_PREFIX is 'Common part of the hashed path of the groupped values';

comment on column XML_ENTITY_SETUP.XPATH_HEAD is 'The begginning part of the entity node xpath, used to select desired attributes under the entity nodes. It may be followed by indexed parts ( from 1 to 6).';

comment on column XML_ENTITY_SETUP.XPATH_PART1 is 'The part 1 of the node''s XPATH, indexed by 1st index. All 6 indexed parts compose the whole XPATH to the entity node. All values under the node will be groupped values to a single file part during the XML file upload';

comment on column XML_ENTITY_SETUP.XPATH_PART2 is 'The part 2 of the node''s XPATH, indexed by 2d index. All 6 indexed parts compose the whole XPATH to the entity node. All values under the node will be groupped values to a single file part during the XML file upload';

comment on column XML_ENTITY_SETUP.XPATH_PART3 is 'The part 3 of the node''s XPATH, indexed by 3d index. All 6 indexed parts compose the whole XPATH to the entity node. All values under the node will be groupped values to a single file part during the XML file upload';

comment on column XML_ENTITY_SETUP.XPATH_PART4 is 'The part 4 of the node''s XPATH, indexed by 4th index. All 6 indexed parts compose the whole XPATH to the entity node. All values under the node will be groupped values to a single file part during the XML file upload';

comment on column XML_ENTITY_SETUP.XPATH_PART5 is 'The part 5 of the node''s XPATH, indexed by 5th index. All 6 indexed parts compose the whole XPATH to the entity node. All values under the node will be groupped values to a single file part during the XML file upload';

comment on column XML_ENTITY_SETUP.XPATH_PART6 is 'The part 6 of the node''s XPATH, indexed by 6th index. All 6 indexed parts compose the whole XPATH to the entity node. All values under the node will be groupped values to a single file part during the XML file upload';

comment on column XML_ENTITY_SETUP.XPATH_TAIL_TEXTS is 'The last part of the entity node xpath, used to select desired attributes under the entity nodes';

comment on column XML_ENTITY_SETUP.XPATH_TAIL_ATTR is 'The last part of the entity node xpath, used to select desired attributes under the entity nodes';

comment on column XML_ENTITY_SETUP.ENTRY_STATUS is '1 - Active0 - Inactive';

comment on column XML_ENTITY_SETUP.FK_SWG_MESSAGE_TYPE is 'MESSAGE_TYPEIncoming or Outgoing SWIFT Message Type :XX: MT-100 (not supported)10: MT-10111: MT-10312: MT-11013: MT-19920: MT-20021: MT-20222: MT-299                                                                                                         ';

create unique index XMLENTI1
    on XML_ENTITY_SETUP (FK_XMLNAMESPACE_DOC_TYPE, FK_XMLNAMESPACE_URN, FK_XMLNAMESPACE_PREFIX);

