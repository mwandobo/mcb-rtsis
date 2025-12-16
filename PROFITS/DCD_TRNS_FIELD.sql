create table DCD_TRNS_FIELD
(
    CODE               CHAR(8)      not null,
    TABLE_ATTRIBUTE    CHAR(40)     not null,
    TABLE_ENTITY       CHAR(40)     not null,
    TMPSTAMP           TIMESTAMP(6) not null,
    PRFT_SYSTEM        SMALLINT,
    DEC_PLACES         INTEGER,
    FIELD_LENGTH       INTEGER,
    LANGUAGE_USED      INTEGER,
    MODEL_ID           DECIMAL(12),
    TYPE_IND           CHAR(1),
    VARYING_LENGTH     CHAR(1),
    FIELD_USED         CHAR(1),
    UPDATE_FIELD       CHAR(1),
    TEMPORARY_FIELD    CHAR(1),
    SECOND_FIELD       CHAR(1),
    FIRST_FIELD        CHAR(1),
    CALC_FIELD         CHAR(1),
    STATUS0            CHAR(1),
    FROM_ENCYCLOPEDIA  CHAR(1),
    CATEGORY_TYPE      CHAR(2),
    OPTIONAL_MANDATORY CHAR(2),
    FIELD_TYPE         CHAR(2),
    PASSWORD           CHAR(26),
    DESCRIPTION        CHAR(40),
    ALIAS_TABLE_ORIGIN CHAR(40),
    RELATIONSHIP_TABLE CHAR(40),
    FUNCTIONALITY_DESC CHAR(240),
    constraint IXU_DEF_122
        primary key (CODE, TABLE_ATTRIBUTE, TABLE_ENTITY, TMPSTAMP)
);

