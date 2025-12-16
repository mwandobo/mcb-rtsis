create table CUST_ADD_INFO_U
(
    FK_CUSTOMERCUST_ID     INTEGER      not null
        constraint IXU_CIU_021
            primary key,
    PROPERTYINFO           VARCHAR(250),
    PROPERTY_OWNER         CHAR(1),
    TMSTAMP                TIMESTAMP(6) not null,
    SALARY_RECORD_NUMBER   CHAR(12),
    INSURANCE_END_DATE     DATE,
    TAX_END_DATE           DATE,
    CORPORATION_START_DATE DATE,
    CORPORATION_END_DATE   DATE,
    LEGAL_END_DATE         DATE,
    END_REASON             CHAR(254),
    TOTAL_EMPLOYEES        INTEGER,
    CO_REPRES_TERMS        CHAR(254),
    QUALIFICATIONS         VARCHAR(40),
    SIGNATURE              VARCHAR(2000)
);

