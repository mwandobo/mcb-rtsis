create table CLC_INT_COLLATERALS
(
    COLL_ID            VARCHAR(32)    not null,
    COLL_TYPE          VARCHAR(32)    not null,
    COLL_CURRENCY      VARCHAR(32)    not null,
    COLL_DESCRIPTION   VARCHAR(2000),
    COLL_MARKETVALUE   DECIMAL(18, 3) not null,
    COLL_AMOUNT        DECIMAL(18, 3) not null,
    COLL_CADASTRE      VARCHAR(50),
    COLL_DATADATE      DATE           not null,
    COLL_EXPORTDATE    DATE           not null,
    TMSTAMP_COLLATERAL TIMESTAMP(6),
    constraint PK_CLC_INT_COLLATE
        primary key (COLL_EXPORTDATE, COLL_ID)
);

comment on table CLC_INT_COLLATERALS is 'This table contains analytic data about collaterals';

comment on column CLC_INT_COLLATERALS.COLL_ID is 'Unique Collateral ID - COLLATERAL_SN';

comment on column CLC_INT_COLLATERALS.COLL_TYPE is 'Collateral Type - COLLATERAL.FK_COLLATERAL_TFK';

comment on column CLC_INT_COLLATERALS.COLL_CURRENCY is 'Currency of collateral values - COLLATERAL.FK_CURRENCYID_CURR (JOIN WITH CURRENCY)';

comment on column CLC_INT_COLLATERALS.COLL_DESCRIPTION is 'Collateral description - COLLATERAL.COLLATERAL_DESC';

comment on column CLC_INT_COLLATERALS.COLL_MARKETVALUE is 'Collateral Market Value - COLLATERAL.TOT_EST_VALUE_AMN';

comment on column CLC_INT_COLLATERALS.COLL_AMOUNT is 'Collateral Amount - COLLATERAL.CURRENT_VALUE';

comment on column CLC_INT_COLLATERALS.COLL_CADASTRE is 'Cadastre / Land Registry () - COLLATERAL.FK_GENERIC_DETASER';

comment on column CLC_INT_COLLATERALS.COLL_DATADATE is 'Date that data refer to - FILLED BY EXPORT APP';

comment on column CLC_INT_COLLATERALS.COLL_EXPORTDATE is 'Export date - FILLED BY EXPORT APP';

