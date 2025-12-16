create table COLLECTION_AGENCY
(
    SN                 INTEGER,
    DEDICATION_LAST_DT DATE,
    SERVICE_FLAG       CHAR(1),
    ENTRY_STATUS       CHAR(1),
    ADDRESS_TK         CHAR(7),
    PHONE              CHAR(15),
    FAX                CHAR(15),
    ADDRESS            CHAR(30),
    ADDRESS_STREET     CHAR(30),
    ADDRESS_CITY       CHAR(30),
    DESCRIPTION        CHAR(100)
);

create unique index IXU_COL_003
    on COLLECTION_AGENCY (SN);

