create table CP_TEMP_UTIL
(
    UNIT             INTEGER,
    TKA_CODE         CHAR(21),
    COUNTR           INTEGER,
    AMOUNT           DECIMAL(15, 2),
    COLLECTION_DATE  DATE,
    TKA_ENVIRONM_TAX CHAR(2),
    TKA_SPECIAL_TAX  CHAR(2),
    TEXT3            CHAR(3),
    TKA_CATEGORY     CHAR(3),
    AFM              CHAR(9),
    TEXT22           CHAR(22),
    GENERIC_FIELD_1  CHAR(30),
    GENERIC_FIELD_2  CHAR(30),
    GENERIC_FIELD_3  CHAR(30),
    GENERIC_FIELD_4  CHAR(30)
);

create unique index IXU_CP__013
    on CP_TEMP_UTIL (UNIT, TEXT22, TKA_CODE);

