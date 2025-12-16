create table DIAS_DTL_PSUM
(
    FK_DIAS_HDR_PSUCRE DATE        not null,
    FK_DIAS_HDR_PSUFIL INTEGER     not null,
    RECORD_SN          DECIMAL(10) not null,
    ACQ_CODE           INTEGER,
    ISSUER_CODE        INTEGER,
    ACQ_FEE            DECIMAL(11, 2),
    DIAS_FEE           DECIMAL(11, 2),
    DEBIT_AMOUNT       DECIMAL(15, 2),
    CREDIT_AMOUNT      DECIMAL(15, 2),
    TIMESTMP           DATE,
    BUSINESS_DATE      DATE,
    constraint IXU_CP_085
        primary key (FK_DIAS_HDR_PSUCRE, FK_DIAS_HDR_PSUFIL, RECORD_SN)
);

