create table DASC
(
    ASSOC_FROM_OBJ_ID DECIMAL(10) not null,
    ASSOC_TYPE_CODE   INTEGER     not null,
    ASSOC_TO_OBJ_ID   DECIMAL(10) not null,
    ASSOC_NEXT_OBJ_ID DECIMAL(10) not null,
    ASSOC_SEQ         DECIMAL(10) not null,
    ASSOC_DEL_PENDING CHAR(1)     not null,
    ASSOC_CHG_STATUS  CHAR(1)     not null,
    ASSOC_DATE        DATE        not null,
    ASSOC_TIME        DATE        not null,
    ASSOC_USER        CHAR(8)     not null,
    ASSOC_MODEL_ID    DECIMAL(10) not null,
    constraint DASCI1
        primary key (ASSOC_FROM_OBJ_ID, ASSOC_TYPE_CODE, ASSOC_TO_OBJ_ID)
);

