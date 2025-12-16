create table DOC_DETAILS
(
    DOC_ID         DECIMAL(10) not null
        constraint IXU_DOC_003
            primary key,
    IMAGE_NUMBER   INTEGER,
    IMAGE_ID       DECIMAL(12),
    TMSTAMP        TIMESTAMP(6),
    ACTIVE_FLAG    CHAR(1),
    FK_CATEGORY_ID CHAR(4),
    PRFT_OBJ_TYPE  CHAR(20),
    PRFT_OBJ_VALUE CHAR(50),
    FILENAME       CHAR(75),
    DESCRIPTION    CHAR(100),
    COMMENTS       CHAR(100)
);

create unique index FKIMAGEID
    on DOC_DETAILS (IMAGE_ID);

