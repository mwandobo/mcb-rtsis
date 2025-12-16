create table SWT_MSG_OBJECT_KEY
(
    MSG_TYPE     CHAR(20)    not null,
    MSG_CATEGORY CHAR(1)     not null,
    MESSAGE_SN   INTEGER     not null,
    TAG          CHAR(10)    not null,
    SUBTAG_SN    SMALLINT    not null,
    OBJECT_ID    DECIMAL(10) not null,
    constraint PK_SWT_OBJ_KEY
        primary key (SUBTAG_SN, TAG, MESSAGE_SN, MSG_CATEGORY, MSG_TYPE, OBJECT_ID)
);

