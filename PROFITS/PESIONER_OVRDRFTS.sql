create table PESIONER_OVRDRFTS
(
    PROFITS_ACCOUNT VARCHAR(40) not null,
    PROJECT         SMALLINT    not null,
    STATUS          SMALLINT,
    DURATION        SMALLINT,
    COMMISIONS      DECIMAL(15, 2),
    GEN_AMOUNT      DECIMAL(15, 2),
    DEC_STEP_AMOUNT DECIMAL(15, 2),
    INS_DATE        DATE,
    FINALIZE_DATE   DATE,
    UPD_DATE        DATE,
    TIMESTMP        TIMESTAMP(6),
    INS_USER        CHAR(8),
    UPD_USER        CHAR(8),
    constraint IXU_DEP_140
        primary key (PROFITS_ACCOUNT, PROJECT)
);

