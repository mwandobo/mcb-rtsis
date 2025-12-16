create table HIST_SPEC_AGR_HD
(
    TRX_DATE       DATE    not null,
    TRX_USER_SN    INTEGER not null,
    TRX_UNIT       INTEGER not null,
    TRX_USER       CHAR(8) not null,
    TMSTAMP        TIMESTAMP(6),
    TRX_FLG        CHAR(1),
    PROD_FLG       CHAR(1),
    JUST_FLG       CHAR(1),
    ENTRY_STATUS   CHAR(1),
    CATEGORY_CODE  CHAR(8),
    PARAMETER_TYPE CHAR(10),
    constraint IXU_CIS_167
        primary key (TRX_DATE, TRX_USER_SN, TRX_UNIT, TRX_USER)
);

