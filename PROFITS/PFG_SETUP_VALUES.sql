create table PFG_SETUP_VALUES
(
    TAG_SET_CODE       CHAR(20) not null,
    TAG                CHAR(10) not null,
    SET_CATEGORY       CHAR(1)  not null,
    SET_SN             INTEGER  not null,
    INTERNAL_SN        INTEGER  not null,
    SUBTAG_SN          SMALLINT,
    NEEDS_TYPE         CHAR(2),
    MANIPULATE_TAG     CHAR(10),
    MANIPULATE_TAG_2   CHAR(10),
    MANIPULATE_SUBTAG  SMALLINT,
    PREDEFINED_VALUES  CHAR(40),
    ADDITIONAL_SWIFT   CHAR(20),
    NUMERIC_FUNCTION   CHAR(2),
    NUMERIC_RESULT     CHAR(10),
    CHAR_FUNCTION      CHAR(20),
    CHAR_TEXT          CHAR(40),
    ALLOW_VALUE        CHAR(40),
    GEN_OPTION         CHAR(5),
    MANIPULATE_SUBTAG2 SMALLINT,
    ENTRY_DESCR        VARCHAR(80),
    constraint PK_PFGVAL
        primary key (TAG_SET_CODE, TAG, SET_SN, INTERNAL_SN, SET_CATEGORY)
);

