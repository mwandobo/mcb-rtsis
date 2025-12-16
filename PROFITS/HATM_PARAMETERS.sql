create table HATM_PARAMETERS
(
    PROGRAM_ID       CHAR(10)     not null,
    ATM_PROTOCOL_ID  SMALLINT,
    RULE_ID          DECIMAL(12),
    BIN              DECIMAL(11),
    TERMINAL         CHAR(4),
    ERROR_CONTROL    CHAR(1),
    DIAS_USR         CHAR(8),
    POS_USR          CHAR(8),
    INTERBANK_FEES   DECIMAL(15, 2),
    VAT              INTEGER,
    GL_RULE_DIAS     INTEGER,
    GL_RULE_OUR_VISA INTEGER,
    GL_RULE_OUR_MCRD INTEGER,
    GL_RULE_OTH_VISA INTEGER,
    GL_RULE_OTH_MCRD INTEGER,
    GL_RULE_OTH_AMEX INTEGER,
    TMSTAMP          TIMESTAMP(6) not null,
    USR_UPD          CHAR(8),
    UPD_DTE          DATE,
    constraint PK_HATMPARAM
        primary key (TMSTAMP, PROGRAM_ID)
);

