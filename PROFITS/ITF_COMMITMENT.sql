create table ITF_COMMITMENT
(
    IDENTIFIER        DECIMAL(10) not null,
    ORGANISATION_CODE CHAR(10)    not null,
    BANK_ID           INTEGER,
    RECEIVE_DATE      DATE,
    TIMESTMP          TIMESTAMP(6),
    ENTRY_STATUS      CHAR(1),
    KEY_FIELD_4       CHAR(30),
    KEY_FIELD_3       CHAR(30),
    KEY_FIELD_1       CHAR(30),
    KEY_FIELD_2       CHAR(30),
    constraint IXU_CP_096
        primary key (IDENTIFIER, ORGANISATION_CODE)
);

