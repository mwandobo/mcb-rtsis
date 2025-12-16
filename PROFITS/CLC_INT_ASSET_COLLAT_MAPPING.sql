create table CLC_INT_ASSET_COLLAT_MAPPING
(
    VAL_ID                   VARCHAR(20) not null,
    COLL_ID                  VARCHAR(32) not null,
    VALCOL_ENCUMBERAMT       DECIMAL(18, 3),
    VALCOL_COLLATRANK        CHAR(2),
    VALCOL_REDUCAMT          DECIMAL(18, 3),
    VALCOL_DATADATE          DATE        not null,
    VALCOL_EXPORTDATE        DATE        not null,
    TMSTAMP_COLLATERAL_TABLE TIMESTAMP(6),
    constraint PK_CLC_INT_ASSET_C
        primary key (VALCOL_EXPORTDATE, VAL_ID)
);

comment on table CLC_INT_ASSET_COLLAT_MAPPING is 'Mapping of Assets to Collaterals. In this way a collateral may be a mixture of assets, each one of them "offering" a portion of its value to the synthesis of the collateral.';

comment on column CLC_INT_ASSET_COLLAT_MAPPING.VAL_ID is 'Asset ID -';

comment on column CLC_INT_ASSET_COLLAT_MAPPING.COLL_ID is 'Collateral ID -';

comment on column CLC_INT_ASSET_COLLAT_MAPPING.VALCOL_ENCUMBERAMT is 'Encumber Amount/  -';

comment on column CLC_INT_ASSET_COLLAT_MAPPING.VALCOL_COLLATRANK is 'Collateral Rank';

comment on column CLC_INT_ASSET_COLLAT_MAPPING.VALCOL_REDUCAMT is 'Reduction Amount/  (   and        )';

comment on column CLC_INT_ASSET_COLLAT_MAPPING.VALCOL_DATADATE is 'Date that data refer to - FILLED BY EXPORT APP';

comment on column CLC_INT_ASSET_COLLAT_MAPPING.VALCOL_EXPORTDATE is 'Export date - FILLED BY EXPORT APP';

