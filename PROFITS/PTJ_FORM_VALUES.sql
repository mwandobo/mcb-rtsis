create table PTJ_FORM_VALUES
(
    PTJ_DTL_VALUE_ID     DECIMAL(10) not null,
    FK_PTJ_HEADER        DECIMAL(10) not null,
    FK_PTJ_FORM_CODE     CHAR(20)    not null,
    FK_GH_GROUP          CHAR(5)     not null,
    FK_GD_GROUP          INTEGER     not null,
    TAG                  CHAR(10)    not null,
    FIELD_VALUE          VARCHAR(100),
    FIELD_ERROR_MESSAGE  VARCHAR(60),
    FIELD_AUTHORIZE_RULE INTEGER,
    CREATE_USER          CHAR(8),
    CREATE_UNIT          INTEGER,
    CREATE_DATE          DATE,
    CREATE_TMSTAMP       TIMESTAMP(6),
    UPDATE_USER          CHAR(8),
    UPDATE_UNIT          INTEGER,
    UPDATE_DATE          DATE,
    UPDATE_TMSTAMP       TIMESTAMP(6),
    ENTRY_STATUS         CHAR(1),
    constraint PK_PTJ_PARAM_3
        primary key (PTJ_DTL_VALUE_ID, FK_PTJ_HEADER, FK_PTJ_FORM_CODE, FK_GH_GROUP, FK_GD_GROUP)
);

