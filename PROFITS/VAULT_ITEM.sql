create table VAULT_ITEM
(
    SN               DECIMAL(12)  not null
        constraint PK_VAULT_ITEM
            primary key,
    ITEM_DESCRIPTION CHAR(40),
    ITEM_DETAILS     VARCHAR(500) not null,
    ITEM_VALUE       DECIMAL(15, 2),
    INSERTED_DATE    DATE,
    REMOVAL_DATE     DATE,
    RETURNED_DATE    DATE,
    RESPONSIBLE_USER CHAR(8),
    CHECKED_DATE     DATE,
    CUST_ID          INTEGER,
    PROFITS_ACCOUNT  CHAR(40),
    ACCOUNT_CD       SMALLINT,
    PROFITS_SYSTEM   SMALLINT,
    VAULT_LOCATION   CHAR(250),
    UPDATE_UNIT      INTEGER,
    ITEM_STATUS      CHAR(1),
    ENTRY_STATUS     CHAR(1),
    UPDATE_USER      CHAR(8),
    UPDATE_DATE      DATE,
    INSERT_UNIT      INTEGER,
    INSERT_USER      CHAR(8),
    INSERT_DATE      DATE,
    TMSTAMP          TIMESTAMP(6),
    FK_GH            CHAR(5),
    FK_GD            INTEGER,
    VAULT_UNIT       INTEGER,
    FILE_EXISTS      CHAR(1),
    INTERNAL_SN      DECIMAL(10),
    RECORD_TYPE      CHAR(2),
    FK_REASON_GH     CHAR(5),
    FK_REASON_GD     INTEGER
);

create unique index I0000188
    on VAULT_ITEM (FK_REASON_GH, FK_REASON_GD);

