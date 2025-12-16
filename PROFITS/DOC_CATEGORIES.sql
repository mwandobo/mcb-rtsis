create table DOC_CATEGORIES
(
    CATEGORY_ID   CHAR(4) not null
        constraint IXU_DOC_002
            primary key,
    ACTIVE_FLAG   CHAR(1),
    FK_SYSTEM_ID  CHAR(4),
    CATEGORY_DESC CHAR(50)
);

