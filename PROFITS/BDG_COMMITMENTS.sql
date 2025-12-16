create table BDG_COMMITMENTS
(
    YEAR0                  DECIMAL(4)     not null,
    FK_UNITCODE            DECIMAL(5)     not null,
    FK_LINE_ID             CHAR(30)       not null
        references BDG_ELEMENT,
    COMMITMENT_SN          DECIMAL(15)    not null,
    FK_CURRENCYID_CURRENCY DECIMAL(5),
    AMOUNT                 DECIMAL(15, 2) not null,
    RATE                   DECIMAL(18, 6),
    AMOUNT_LC              DECIMAL(18, 2),
    JUSTIFICATION          VARCHAR(500),
    PROPONENT              DECIMAL(5),
    IDENTIFICATION_NUM     VARCHAR(100),
    ENTRY_STATUS           CHAR(1),
    AUTH_STATUS            CHAR(1),
    TRX_USER_INS           CHAR(8),
    TRX_UNIT_INS           DECIMAL(5),
    TRX_DATE_INS           DATE,
    CREATE_TMSTAMP         TIMESTAMP(6),
    TRX_USER_UPD           CHAR(8),
    TRX_UNIT_UPD           DECIMAL(5),
    TRX_DATE_UPD           DATE,
    UPD_TMSTAMP            TIMESTAMP(6),
    UPD_DESCRIPTION        VARCHAR(80),
    TRX_USER_AUTH          CHAR(8),
    TRX_UNIT_AUTH          DECIMAL(5),
    TRX_DATE_AUTH          DATE,
    AUTH_TMSTAMP           TIMESTAMP(6),
    AUTH_DESCRIPTION       VARCHAR(80),
    FILENAME               VARCHAR(150),
    constraint IXU_BDG_COMMITMENTS
        primary key (YEAR0, FK_UNITCODE, FK_LINE_ID, COMMITMENT_SN)
);

