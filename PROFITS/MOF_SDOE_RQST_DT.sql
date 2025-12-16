create table MOF_SDOE_RQST_DT
(
    RECORD_TYPE        CHAR(2),
    BATCH_ID           CHAR(12) not null,
    UNIQUE_REF_ID      CHAR(12) not null,
    MOF_TAXID          CHAR(9),
    RQST_AFM_SN        CHAR(3),
    CASE_SN            CHAR(9),
    MOF_ALGORITHM      CHAR(1),
    MOF_IDDOC_TYPE     CHAR(2),
    MOF_IDDOC_NUM      CHAR(20),
    MOF_EOP_STRING     CHAR(10),
    REQST_TIMESTMP     CHAR(26),
    MOF_INCOMING_ID    CHAR(12),
    MOF_FOREAS         CHAR(50),
    MOG_LEGISLATION_NO CHAR(30),
    MOF_VOULEUMA       CHAR(20),
    MOF_PROTOCOL_NO    CHAR(20),
    MOF_REF_DATE_FROM  DATE,
    MOF_REF_DATE_TO    DATE,
    MOF_SURNAME_TITLE  CHAR(70),
    MOF_FIRST_NAME     CHAR(20),
    MOF_FATHERNAME     CHAR(20),
    MOF_MOTHERNAME     CHAR(20),
    MOF_BIRTH_DATE     CHAR(10),
    MOF_ADDRESS_HOME   CHAR(50),
    constraint PK_RQST_DT
        primary key (UNIQUE_REF_ID, BATCH_ID)
);

