create table ISSU_PRD_REP
(
    ID_PRODUCT  INTEGER,
    PATH        CHAR(40),
    FILENAME    CHAR(70),
    DESCRIPTION CHAR(70)
);

create unique index IXP_ISS_000
    on ISSU_PRD_REP (ID_PRODUCT, PATH, FILENAME);

