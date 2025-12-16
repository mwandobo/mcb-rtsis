create table TR_AMK_HDR
(
    AMK_CODE           INTEGER not null
        constraint IXU_DEP_161
            primary key,
    DENOMINATION       SMALLINT,
    ID_CURRENCY        INTEGER,
    FKGD_STORE         INTEGER,
    FK_JUSTIFICID_JUST INTEGER,
    FK_TR_AGENDA_CODE  INTEGER,
    RATIO1             DECIMAL(12, 8),
    RATIO2             DECIMAL(12, 8),
    PRICE_PER_SHARE    DECIMAL(15, 2),
    EX_PERIOD_FROM     DATE,
    CREATION_DATE      DATE,
    ANNOUNCE_DATE      DATE,
    EX_PERIOD_TO       DATE,
    TMSTAMP            TIMESTAMP(6),
    NEXT_WORK_DATE     DATE,
    RECORD_DATE        DATE,
    EXPIRY_DATE        DATE,
    PAY_DATE           DATE,
    COMPLETE_DATE      DATE,
    RESPONSE_DATE      DATE,
    AMK_STATUS         CHAR(1),
    SALABLE            CHAR(1),
    ODD_LOTS           CHAR(1),
    RIGHT_BONUS        CHAR(1),
    CALC_METHOD        CHAR(1),
    FKGH_STORE         CHAR(5),
    FK_RIGHTS_TRBOND   CHAR(15),
    FK_NONTRADE_TRBOND CHAR(15),
    FK_AMK_TRBOND      CHAR(15),
    AMK_DESC           CHAR(40),
    ENTRY_COMMENTS     VARCHAR(250)
);

