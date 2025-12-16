create table HIST_XML_INPUT_FILE_DATA
(
    ID             DECIMAL(15) not null
        constraint HISTXMLIFDPK
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

create unique index HISTUAXMLIFDI1
    on HIST_XML_INPUT_FILE_DATA (FK_XML_FILE_ID);

