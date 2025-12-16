create table SPEC_AGR_HEADER
(
    PARAMETER_TYPE     CHAR(10) not null,
    CATEGORY_CODE      CHAR(8)  not null,
    TMSTAMP            TIMESTAMP(6),
    TRX_FLG            CHAR(1),
    ENTRY_STATUS       CHAR(1),
    JUST_FLG           CHAR(1),
    PROD_FLG           CHAR(1),
    AGREED_INCOME_AMN  INTEGER,
    DURATION_NUMBER    INTEGER,
    DURATION_UNIT      CHAR(1),
    AGREEMENT_COMMENTS VARCHAR(500),
    FK_GH_TYPE         CHAR(1),
    FK_GD_SN           INTEGER,
    constraint IXU_SPE_000
        primary key (PARAMETER_TYPE, CATEGORY_CODE)
);

