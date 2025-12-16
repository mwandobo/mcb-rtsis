create table BDG_GEN_PARAMETERS
(
    ID                     CHAR(2) not null,
    STATUS                 SMALLINT,
    START_YEAR             SMALLINT,
    LAST_DEL_YEAR          SMALLINT,
    CURRENT_YEAR           SMALLINT,
    TMSTMP                 DATE,
    DISTRICT_TO_DATE       DATE,
    ADMIN_TO_DATE          DATE,
    UNIT_TO_DATE           DATE,
    ADMIN_FROM_DATE        DATE,
    UNIT_FROM_DATE         DATE,
    DISTRICT_FROM_DATE     DATE,
    FK_BDG_PHASEID         CHAR(2),
    YEARS                  INTEGER,
    BDG_DURATION_STEP      SMALLINT,
    BDG_DURATION_UNIT_TIME SMALLINT,
    WAY_EXPRESSED_AMNS     INTEGER,
    BUDGET_CURRENCY        SMALLINT default '1',
    FK_PROFILE_BUDGET      CHAR(8),
    FK_USR_BUDGET          CHAR(8),
    FK_BDG_PHASESTAGE_ID   INTEGER not null
);

create unique index I0000523
    on BDG_GEN_PARAMETERS (FK_BDG_PHASEID, FK_BDG_PHASESTAGE_ID);

