create table ASSET_MASTER_MOVABLE
(
    FK_ASSET_ID        VARCHAR(10) not null
        constraint IXU_AMM_TRX
            primary key,
    PLATE_NO           VARCHAR(10),
    VIN                VARCHAR(17),
    MODEL_ID           DECIMAL(5),
    USAGE_ID           DECIMAL(5),
    TIRE_CODE_ID       DECIMAL(5),
    FIRST_LICENSE_DATE DATE,
    LAST_LICENSE_DATE  DATE,
    COLOR              DECIMAL(5),
    INTERIOR_COLOR     DECIMAL(5),
    EQUIPMENT          VARCHAR(500),
    HORSE_POWER        DECIMAL(5),
    WEIGHT             DECIMAL(7, 2),
    DELIVERY_DATE      DATE,
    USAGE_CITY         DECIMAL(5),
    USEFUL_LOAD        DECIMAL(7, 2),
    SUSPENSION_ID      VARCHAR(30),
    AXES_ID            VARCHAR(30),
    MAX_LOAD           DECIMAL(7, 2),
    SELLING_PRICE      DECIMAL(18, 2),
    ECO_CERTIFICATE    VARCHAR(30),
    ENGINE_NO          VARCHAR(30),
    KILOMETERS         DECIMAL(10, 2)
);

