create table CLC_INT_MASTER_DATA
(
    MD_TABLENAME  CHAR(50)    not null,
    MD_TABLEKEY   VARCHAR(32) not null,
    MD_VALUE1     VARCHAR(50) not null,
    MD_VALUE2     VARCHAR(50),
    MD_DATADATE   DATE        not null,
    MD_EXPORTDATE DATE        not null,
    constraint PK_CLC_INT_MASTER
        primary key (MD_TABLEKEY, MD_EXPORTDATE, MD_TABLENAME)
);

comment on table CLC_INT_MASTER_DATA is 'This table contains analytic data about LOV (List Of Values)';

comment on column CLC_INT_MASTER_DATA.MD_TABLENAME is 'Category of master data        BANKID: Deposit Bank IDBRANCH: Branches of BankDNOTYPE: ID typesPRODUCT: ProductsCURRENCY: Exchange ratesCOLTYPE: Collateral typesTRNCODE: Transaction typesNACE: Codes of economic activitiesARGTYPE: Modification TypeARGCAT';

comment on column CLC_INT_MASTER_DATA.MD_TABLEKEY is 'Key of table -';

comment on column CLC_INT_MASTER_DATA.MD_VALUE1 is 'Value 1  Defined for each Table -';

comment on column CLC_INT_MASTER_DATA.MD_VALUE2 is 'Value 2  Defined for each Table -';

comment on column CLC_INT_MASTER_DATA.MD_DATADATE is 'Date that data refer to - FILLED BY EXPORT APP';

comment on column CLC_INT_MASTER_DATA.MD_EXPORTDATE is 'Export date - FILLED BY EXPORT APP';

