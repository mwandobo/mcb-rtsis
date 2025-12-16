create table ISS_COMMITMENT_APP
(
    DOC_ID           INTEGER,
    TP_SO_IDENTIFIER DECIMAL(10),
    ID_APPR          INTEGER,
    APPR_DATE        DATE,
    ENTRY_STATUS     CHAR(1),
    APPR_USER        CHAR(8)
);

create unique index IXU_ISS_019
    on ISS_COMMITMENT_APP (DOC_ID, TP_SO_IDENTIFIER);

