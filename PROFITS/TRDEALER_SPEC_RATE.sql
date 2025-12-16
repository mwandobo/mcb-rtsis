create table TRDEALER_SPEC_RATE
(
    FK_USRCODE      CHAR(8)     not null,
    ENTRY_DATE      DATE        not null,
    DEALER_REF_NO   DECIMAL(10) not null,
    TAX_FLG         SMALLINT,
    DAYS_DURATION   SMALLINT,
    TRX_UNIT        INTEGER,
    TRX_USER_SN     INTEGER,
    SPECIAL_RATE    DECIMAL(9, 6),
    INT_ACCR        DECIMAL(14, 8),
    REFERENCE_VALUE DECIMAL(14, 8),
    NET_PRICE       DECIMAL(14, 8),
    TRANS_AMOUNT    DECIMAL(15, 2),
    FACE_VALUE      DECIMAL(18, 3),
    TRX_DATE        DATE,
    VALEUR          DATE,
    ENTRY_STATUS    CHAR(1),
    TRX_USER        CHAR(8),
    ANAGGELIA       CHAR(8),
    BOND_CODE       CHAR(15),
    ENTRY_COMMENTS  VARCHAR(40),
    constraint IXU_DEP_151
        primary key (FK_USRCODE, ENTRY_DATE, DEALER_REF_NO)
);

