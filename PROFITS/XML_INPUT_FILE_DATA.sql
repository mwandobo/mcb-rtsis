create table XML_INPUT_FILE_DATA
(
    ID             DECIMAL(15) not null
        constraint XMLIFDPK
            primary key,
    HASH_KEY       DECIMAL(15) not null,
    HASH_SEQ_ID    INTEGER     not null,
    HASHED_XPATH   VARCHAR(500),
    NAMESPACE      CHAR(128),
    IS_ATTRIBUTE   CHAR(1),
    ACTUAL_XPATH   VARCHAR(500),
    VALUE0         VARCHAR(2000),
    INDEX1         INTEGER,
    INDEX2         INTEGER,
    INDEX3         INTEGER,
    INDEX4         INTEGER,
    INDEX5         INTEGER,
    INDEX6         INTEGER,
    FK_XML_FILE_ID DECIMAL(15)
);

comment on column XML_INPUT_FILE_DATA.ID is 'Uninue ID, a number from an ascending sequence of integers';

comment on column XML_INPUT_FILE_DATA.HASH_KEY is 'A hash value calculated on the Xpath string. Expected to be unique and match to some setup entry in the XML_NODE_PATH_SETUP';

comment on column XML_INPUT_FILE_DATA.HASH_SEQ_ID is 'Hash sequentional id, used in case if the same Hash is generated for different XPATHs. The pair of the the Hash and the HASH_SEQ_ID is Unique.';

comment on column XML_INPUT_FILE_DATA.HASHED_XPATH is 'The text used for calculating the HASH_KEY during the XML file loading. Consists from the Xpath with standard namespaces''s prefixes, that leads to the node. This XPATH doesn''t contain the interation indeces.';

comment on column XML_INPUT_FILE_DATA.ACTUAL_XPATH is 'This is the actual Xpath with standard namespaces''s prefixes, that leads to the node. This XPATH it contains interation indeces. Fot the input documents it is derived from the actual document, and for the outout documents, it is calculated.';

comment on column XML_INPUT_FILE_DATA.INDEX1 is 'In the indexed Xpaths it is the value of the 1st right index';

comment on column XML_INPUT_FILE_DATA.INDEX2 is 'In the indexed Xpaths it is the value of the 2d right index';

comment on column XML_INPUT_FILE_DATA.INDEX3 is 'In the indexed Xpaths it is the value of the 3d right index';

comment on column XML_INPUT_FILE_DATA.INDEX4 is 'In the indexed Xpaths it is the value of the 4th right index';

comment on column XML_INPUT_FILE_DATA.INDEX5 is 'In the indexed Xpaths it is the value of the 5th right index';

comment on column XML_INPUT_FILE_DATA.INDEX6 is 'In the indexed Xpaths it is the value of the 6th right index';

create unique index UAXMLIFDI1
    on XML_INPUT_FILE_DATA (FK_XML_FILE_ID);

