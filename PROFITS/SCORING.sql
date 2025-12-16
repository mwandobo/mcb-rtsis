create table SCORING
(
    SC_SN                          DECIMAL(10) not null
        constraint PIXSCORI
            primary key,
    CUST_ID                        INTEGER     not null,
    SCORING_STATUS                 CHAR(1)     not null,
    FK_CSC_APPLICATFKGH_HAS_AS_LDE CHAR(5),
    FK_CSC_APPLICATFKGD_HAS_AS_LDE INTEGER,
    FK_CSC_APPLICATFK_UNITCODE     INTEGER,
    FK_CSC_APPLICATAPP_SN          INTEGER,
    FK_CSC_APPLICATSC_YEAR         SMALLINT
);

