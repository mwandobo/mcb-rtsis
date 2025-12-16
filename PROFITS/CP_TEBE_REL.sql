create table CP_TEBE_REL
(
    NEW_CODE        CHAR(30),
    PREVIOUS_CODE   CHAR(30),
    CP_AGREEMENT_NO DECIMAL(10),
    COMMENTS        CHAR(100)
);

create unique index IXU_CP__008
    on CP_TEBE_REL (NEW_CODE, PREVIOUS_CODE);

