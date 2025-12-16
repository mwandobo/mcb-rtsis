create table DMDL
(
    MODEL_ID           DECIMAL(10) not null
        constraint DMDLI1
            primary key,
    MODEL_TYPE         CHAR(1)     not null,
    MODEL_NAME         CHAR(32)    not null,
    MODEL_STATUS       CHAR(1)     not null,
    MODEL_RELEASE      CHAR(8)     not null,
    MODEL_OWNER_ID     CHAR(8)     not null,
    MODEL_CR_USER_ID   CHAR(8)     not null,
    MODEL_CR_DATE      DATE        not null,
    MODEL_CR_TIME      DATE        not null,
    MODEL_USER_ID      CHAR(8)     not null,
    MODEL_DATE         DATE        not null,
    MODEL_TIME         DATE        not null,
    MODEL_PARENT_ENCY  DECIMAL(10),
    MODEL_PARENT_MODEL DECIMAL(10),
    MODEL_PARENT_CKO   DECIMAL(10),
    MODEL_PARENT_DATE  DATE,
    MODEL_PARENT_TIME  DATE,
    MODEL_CODE_PAGE    INTEGER     not null,
    MODEL_LANG_CODE    INTEGER     not null
);

