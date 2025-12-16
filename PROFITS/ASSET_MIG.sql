create table ASSET_MIG
(
    ASSET_ID          CHAR(10) not null
        constraint PKASSET_MIG
            primary key,
    UNIT              INTEGER,
    SUPPL_CODE        CHAR(21),
    ASSET_CAT_DESC    VARCHAR(100),
    ASSET_DESC        VARCHAR(100),
    QUANTITY          DECIMAL(15, 2),
    ACQ_DATE          DATE,
    START_DEPREC_DATE DATE,
    LAST_DEPREC_DATE  DATE,
    ASSET_VALUE       DECIMAL(15, 2),
    TOTAL_DEPREC      DECIMAL(15, 2),
    UNDEPRECIATED     DECIMAL(15, 2),
    SERIAL_NUMBER     VARCHAR(100),
    CHARGED_ADMIN     INTEGER,
    ADDITIONAL_INFO   VARCHAR(2000)
);

