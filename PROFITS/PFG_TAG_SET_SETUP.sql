create table PFG_TAG_SET_SETUP
(
    TAG_SET_CODE       CHAR(20) not null
        constraint PK_PFGSET
            primary key,
    DESCRIPTION        CHAR(40),
    SET_DESCRIPTION    VARCHAR(4000),
    DEFAULT_TAG_SETUP  CHAR(1),
    SET_TYPE           CHAR(2),
    INACTIVE_STATUS    CHAR(1),
    ENTRY_STATUS       CHAR(1),
    SETUP_FORM_PAR     CHAR(5),
    SETUP_FORM_SN      INTEGER,
    DCD_PRFT_SYS_PAR   CHAR(5),
    USR_NOT_EDITABLE   CHAR(1),
    DCD_RULE_ID        DECIMAL(12),
    DCD_PRFT_SYSTEM    SMALLINT,
    SET_HISTORY        CHAR(1),
    SET_MANDATORY_FORM CHAR(1)
);

