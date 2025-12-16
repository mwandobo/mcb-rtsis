create table DTXT
(
    TEXT_OBJ_ID     DECIMAL(10)   not null,
    TEXT_PROP_CODE  INTEGER       not null,
    TEXT_SEQ        INTEGER       not null,
    TEXT_VALUE      VARCHAR(2048) not null,
    TEXT_CHG_STATUS CHAR(1)       not null,
    TEXT_DATE       DATE          not null,
    TEXT_TIME       TIME          not null,
    TEXT_USER       CHAR(8)       not null,
    TEXT_MODEL_ID   DECIMAL(10)   not null,
    constraint DTXTI1
        primary key (TEXT_OBJ_ID, TEXT_PROP_CODE, TEXT_SEQ)
);

