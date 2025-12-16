create table CLC_INT_CONTRACT_LEVEL_INFO
(
    ACC_CONTRACTNUM   VARCHAR(20) not null,
    BP_CIF            VARCHAR(20) not null,
    ACC_INITAPPRDATE  DATE,
    ACC_CONTRACTAMT   DECIMAL(18, 3),
    ACC_CONTRACTDATE  DATE,
    ACC_CONTRACTLIM   VARCHAR(50),
    ACC_OPENACCLIM    VARCHAR(50),
    ACC_LOANLIM       VARCHAR(50),
    ACC_GUARLIM       VARCHAR(50),
    ACC_CONSTATUS     CHAR(1),
    ACC_CLOSEDATE     DATE,
    ACC_DATADATE      DATE        not null,
    ACC_EXPORTDATE    DATE        not null,
    TMSTAMP_AGREEMENT TIMESTAMP(6),
    constraint PK_CLC_INT_CONTRAC
        primary key (ACC_EXPORTDATE, ACC_CONTRACTNUM)
);

comment on table CLC_INT_CONTRACT_LEVEL_INFO is 'This table contains information about a contract of a Business Party (customer)';

comment on column CLC_INT_CONTRACT_LEVEL_INFO.ACC_CONTRACTNUM is 'Contract number -';

comment on column CLC_INT_CONTRACT_LEVEL_INFO.BP_CIF is 'Customer Code (Prime Holder) -';

comment on column CLC_INT_CONTRACT_LEVEL_INFO.ACC_INITAPPRDATE is 'Initial Approval Date (of decision) -';

comment on column CLC_INT_CONTRACT_LEVEL_INFO.ACC_CONTRACTAMT is 'Contract Amount -';

comment on column CLC_INT_CONTRACT_LEVEL_INFO.ACC_CONTRACTDATE is 'Contract Date -';

comment on column CLC_INT_CONTRACT_LEVEL_INFO.ACC_CONTRACTLIM is 'Contract Limit -';

comment on column CLC_INT_CONTRACT_LEVEL_INFO.ACC_OPENACCLIM is 'Open Accounts Limit -';

comment on column CLC_INT_CONTRACT_LEVEL_INFO.ACC_LOANLIM is 'Loan Limit -';

comment on column CLC_INT_CONTRACT_LEVEL_INFO.ACC_GUARLIM is 'Guarantee Limit -';

comment on column CLC_INT_CONTRACT_LEVEL_INFO.ACC_CONSTATUS is 'Contract Status: Open/ (1) Permanent Delay/   (2) Closed/ (3)';

comment on column CLC_INT_CONTRACT_LEVEL_INFO.ACC_CLOSEDATE is 'Close Date -';

comment on column CLC_INT_CONTRACT_LEVEL_INFO.ACC_DATADATE is 'Date that data refer to - FILLED BY EXPORT APP';

comment on column CLC_INT_CONTRACT_LEVEL_INFO.ACC_EXPORTDATE is 'Export date - FILLED BY EXPORT APP';

