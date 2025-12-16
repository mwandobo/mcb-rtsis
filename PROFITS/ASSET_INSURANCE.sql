create table ASSET_INSURANCE
(
    FK_ASSET_ID          VARCHAR(10) not null,
    CODE                 DECIMAL(10) not null,
    CONTRACT_NO          VARCHAR(40),
    INSURANCE_COMPANY_ID DECIMAL(5),
    INSURANCE_AGENCY_ID  DECIMAL(5),
    INSURANCE_TYPE_ID    DECIMAL(5)  not null,
    START_DATE           DATE        not null,
    END_DATE             DATE        not null,
    RENEWAL_DATE         DATE,
    FK_ID_CURRENCY       DECIMAL(5),
    INSURED_VALUE        DECIMAL(18, 2),
    INSURED_VALUE_DC     DECIMAL(18, 2),
    PREMIUM              DECIMAL(18, 2),
    COVERAGE             DECIMAL(18, 2),
    POLICY_NUMBER        VARCHAR(40),
    FIXING_RATE          DECIMAL(12, 6),
    AUX_CODE             VARCHAR(30),
    PAID_TO              DECIMAL(1),
    GROUP_POLICY         DECIMAL(1),
    COMMENTS             VARCHAR(500),
    constraint IXU_AINS_TRX
        primary key (FK_ASSET_ID, CODE)
);

