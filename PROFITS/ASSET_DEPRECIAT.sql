create table ASSET_DEPRECIAT
(
    "ASset_ID"        VARCHAR(10),
    EXTRA_DEPREC_CODE VARCHAR(4) not null,
    TRX_UNIT          INTEGER,
    TRX_LUNIT         INTEGER,
    TRX_LDATE         DATE,
    TRX_DATE          DATE,
    TRX_USR           CHAR(8),
    TRX_LUSR          CHAR(8),
    ADDITIONAL_INFO   VARCHAR(3000),
    ASSET_ID          VARCHAR(10)
);

create unique index PKASSET_DEPRECIAT
    on ASSET_DEPRECIAT (ASSET_ID, EXTRA_DEPREC_CODE);

