create table DCD_FIELD_TMP
(
    ATTRIBUTE0         CHAR(40)    not null,
    ENTITY_TYPE        CHAR(40)    not null,
    LANGUAGE_USED      INTEGER     not null,
    NUM_OCCUR          DECIMAL(15) not null,
    PRFT_SYSTEM        SMALLINT,
    DEC_PLACES         INTEGER,
    LENGTH0            INTEGER,
    MODEL_ID           DECIMAL(12),
    UPDATE_FIELD       CHAR(1),
    TYPE_IND           CHAR(1),
    FROM_ENCYCLOPEDIA  CHAR(1),
    VARYING_LENGTH     CHAR(1),
    FIELD_USED         CHAR(1),
    CHECK0             CHAR(1),
    TEMPORARY_FIELD    CHAR(1),
    SECOND_FIELD       CHAR(1),
    FIRST_FIELD        CHAR(1),
    CALC_FIELD         CHAR(1),
    TYPE0              CHAR(2),
    OPT                CHAR(2),
    DOMAIN0            CHAR(2),
    RELATIONSHIP_TABLE CHAR(40),
    DESCRIPTION        CHAR(40),
    FUNCTIONALITY_DESC VARCHAR(1700),
    constraint IXU_DEF_101
        primary key (ATTRIBUTE0, ENTITY_TYPE, LANGUAGE_USED, NUM_OCCUR)
);

