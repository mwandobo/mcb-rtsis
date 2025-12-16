create table EXDEALS
(
    DEAL   INTEGER,
    RATE   DECIMAL(8, 5),
    AMTFX  DECIMAL(13),
    AMTDR  DECIMAL(15),
    VALEUR DATE,
    KDAT   DATE,
    AP     CHAR(1),
    STATUS CHAR(1),
    CURCOD CHAR(3),
    BR     CHAR(3)
);

create unique index IXU_EXD_001
    on EXDEALS (DEAL);

