create table CLC_INT_ACCOUNTS_BPARTIES_IDR
(
    IDR_ID                      VARCHAR(40) not null
        constraint PK_CLC_INT_ACCNTS0
            primary key,
    IDR_TYPE_CHANGE             CHAR(1)     not null,
    IDR_OLDACCREFNO             VARCHAR(17),
    IDR_NEWOFFID                INTEGER,
    IDR_NEWACCREFNO             VARCHAR(17),
    IDR_OLDCIF                  VARCHAR(17),
    IDR_NEWCIF                  VARCHAR(17),
    IDR_CONTRACTNUM             VARCHAR(20),
    IDR_DATADATE                DATE        not null,
    IDR_EXPORTDATE              DATE        not null,
    TMSTAMP_LOAN_MOVE_PRODUCT   TIMESTAMP(6),
    TMSTAMP_AGR_ADDITIONAL_ACTL TIMESTAMP(6)
);

comment on table CLC_INT_ACCOUNTS_BPARTIES_IDR is 'This table caters for any changes on Customer IDs, Account Numbers or Product of the accounts already transferred to AroTRON. Using this information the changes are applied before running the main update, and prohibit the creation of new accounts, and en';

comment on column CLC_INT_ACCOUNTS_BPARTIES_IDR.IDR_ID is 'Unique Id - Extra Field to compose PKEY';

comment on column CLC_INT_ACCOUNTS_BPARTIES_IDR.IDR_TYPE_CHANGE is 'Type of Change:- C for CIF,- A for account,- P for product,- X change of CIF of contract- R history of restructuring- F history of forbearance';

comment on column CLC_INT_ACCOUNTS_BPARTIES_IDR.IDR_OLDACCREFNO is 'Existing Account number -';

comment on column CLC_INT_ACCOUNTS_BPARTIES_IDR.IDR_NEWOFFID is 'New product -';

comment on column CLC_INT_ACCOUNTS_BPARTIES_IDR.IDR_NEWACCREFNO is 'New Account number -';

comment on column CLC_INT_ACCOUNTS_BPARTIES_IDR.IDR_OLDCIF is 'Existing CIF -';

comment on column CLC_INT_ACCOUNTS_BPARTIES_IDR.IDR_NEWCIF is 'New CIF -';

comment on column CLC_INT_ACCOUNTS_BPARTIES_IDR.IDR_CONTRACTNUM is 'Contract Number -';

comment on column CLC_INT_ACCOUNTS_BPARTIES_IDR.IDR_DATADATE is 'Date that data refer to - FILLED BY EXPORT APP';

comment on column CLC_INT_ACCOUNTS_BPARTIES_IDR.IDR_EXPORTDATE is 'Export date - FILLED BY EXPORT APP';

