create table RESP_PENBP_SU
(
    REGKOD  INTEGER not null,
    RAKOD   CHAR(2) not null,
    ANOM    CHAR(5) not null,
    MT2     INTEGER,
    MT1     INTEGER,
    MT      INTEGER,
    PERIODI INTEGER,
    DU      INTEGER,
    BN      INTEGER,
    MANY    DECIMAL(5, 2),
    DAB     DATE,
    IND     CHAR(4),
    KNN     CHAR(5),
    NOM     CHAR(7),
    SNN     CHAR(9),
    SSNOM   CHAR(11),
    SB_N    CHAR(12),
    RANAM   CHAR(15),
    SA      CHAR(15),
    AN_N    CHAR(16),
    GV      CHAR(20),
    MISAM   CHAR(36),
    constraint IXU_CP_069
        primary key (REGKOD, RAKOD, ANOM)
);

