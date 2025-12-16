create table SWT_MSG_OBJECT_ATR
(
    PRFT_REF_NO   CHAR(16)    not null,
    TRX_REF_NO_20 CHAR(16)    not null,
    MSG_TYPE      CHAR(20)    not null,
    MSG_CATEGORY  CHAR(1)     not null,
    MESSAGE_SN    INTEGER     not null,
    SN            INTEGER     not null,
    TAG           CHAR(10)    not null,
    SUBTAG_SN     SMALLINT    not null,
    OBJECT_ID     DECIMAL(10) not null,
    constraint PK_SWT_OBJ_ATTR
        primary key (OBJECT_ID, SUBTAG_SN, TAG, SN, PRFT_REF_NO)
);

