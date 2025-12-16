create table CLC_INT_ACCNTS_SECURED_BY_COLL
(
    CRELACC_ID           VARCHAR(32)    not null,
    COLL_ID              CHAR(32)       not null,
    CONTR_ID             VARCHAR(32)    not null,
    ACC_ID               VARCHAR(17),
    CRELACC_AMTALLOC2ACC DECIMAL(18, 3) not null,
    CRELACC_DATADATE     DATE           not null,
    CRELACC_EXPORTDATE   DATE           not null,
    TMSTAMP_COLLATERAL   TIMESTAMP(6),
    constraint PK_CLC_INT_ACCNTS
        primary key (CRELACC_EXPORTDATE, CRELACC_ID)
);

comment on table CLC_INT_ACCNTS_SECURED_BY_COLL is 'This table contains analytic data about accounts secured by collaterals';

comment on column CLC_INT_ACCNTS_SECURED_BY_COLL.CRELACC_ID is 'Unique ID of account - collateral relation';

comment on column CLC_INT_ACCNTS_SECURED_BY_COLL.COLL_ID is 'Collateral ID -';

comment on column CLC_INT_ACCNTS_SECURED_BY_COLL.CONTR_ID is 'Contract No -';

comment on column CLC_INT_ACCNTS_SECURED_BY_COLL.ACC_ID is 'Account No (for PDCs) -';

comment on column CLC_INT_ACCNTS_SECURED_BY_COLL.CRELACC_AMTALLOC2ACC is 'Amount of collateral allocated to account -';

comment on column CLC_INT_ACCNTS_SECURED_BY_COLL.CRELACC_DATADATE is 'Date that data refer to - FILLED BY EXPORT APP';

comment on column CLC_INT_ACCNTS_SECURED_BY_COLL.CRELACC_EXPORTDATE is 'Export date - FILLED BY EXPORT APP';

