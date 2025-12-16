create table COLLATERAL_DETAIL
(
    CTBL_INTERNAL_SN      DECIMAL(10) not null,
    RECORD_TYPE           CHAR(2)     not null,
    INTERNAL_SN           DECIMAL(10) not null,
    REAL_ESTATE_ID        DECIMAL(10),
    COMMENTS              CHAR(254),
    FK_GD_HAS_AS_CARRI    INTEGER,
    FK_GD_HAS_LAND_REG    INTEGER,
    FK_GD_HAS_SERIAL      INTEGER,
    FK_GH_HAS_AS_CARRI    CHAR(5),
    FK_GH_HAS_LAND_REG    CHAR(5),
    FK_GH_HAS_SERIAL      CHAR(5),
    LAWYER                VARCHAR(40),
    MAIN_CONNECT_IND      CHAR(1),
    PRENOTATION_AMN       DECIMAL(15, 2),
    REMOVAL_DATE          DATE,
    REMOVAL_IND           CHAR(1),
    SHEET                 VARCHAR(40),
    VOLUME                VARCHAR(40),
    INSERT_DT             DATE,
    UPDATE_DT             DATE,
    COL_DET_SN            DECIMAL(15),
    LND_REGISTRY_INSRT_DT DATE,
    constraint IXU_COL_038
        primary key (INTERNAL_SN, RECORD_TYPE, CTBL_INTERNAL_SN)
);

