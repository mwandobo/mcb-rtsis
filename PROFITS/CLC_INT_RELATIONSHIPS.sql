create table CLC_INT_RELATIONSHIPS
(
    REL_ID               VARCHAR(37) not null,
    REL_SCENARIO         VARCHAR(10) not null,
    BP_SOURCEID          VARCHAR(20) not null,
    ACC_ID               VARCHAR(17) not null,
    BP_TARGETID          VARCHAR(17) not null,
    CONTRACT_ID          VARCHAR(17),
    REL_TYPE             VARCHAR(22) not null,
    REL_RANK             SMALLINT,
    REL_STATUS           SMALLINT,
    REL_GUARAMOUNT       DECIMAL(18, 3),
    REL_DATADATE         DATE        not null,
    REL_EXPORTDATE       DATE        not null,
    TMSTAMP_RELATIONSHIP TIMESTAMP(6),
    constraint PK_CLC_INT_RELATIO
        primary key (REL_TYPE, REL_SCENARIO, BP_TARGETID, BP_SOURCEID, REL_EXPORTDATE, REL_ID)
);

comment on table CLC_INT_RELATIONSHIPS is 'This table contains information about the "connections" among Business Parties, as well as "links" of Business Parties to their accounts.';

comment on column CLC_INT_RELATIONSHIPS.REL_ID is 'Unique Key (ACC_ID + BP_ID REL Code) - PROFITS_ACCOUNT.ACCOUNT_NUMBER';

comment on column CLC_INT_RELATIONSHIPS.REL_SCENARIO is 'Supported scenarios:- BP - BP- BP - Contract- BP - Account';

comment on column CLC_INT_RELATIONSHIPS.BP_SOURCEID is 'BP_ID of customer - Case 2:BP-CON -> GuarantorBP_ID of customer AGREEMENT_BENEF. FK_CUSTOMERCUST_ID (MAIN_BENEF_FLG=1)Other CasesRELATIONSHIP.FKCUST_HAS_AS_FIRS';

comment on column CLC_INT_RELATIONSHIPS.ACC_ID is 'Customer related Account Number - PROFITS_ACCOUNT.ACCOUNT_NUMBER';

comment on column CLC_INT_RELATIONSHIPS.BP_TARGETID is 'Target BP_ID of customer - 2.BP-CON -> GuarantorTarget BP_ID of customer -> AGREEMENT_GUARANT. FK_CUSTOMERCUST_IDOther CasesRELATIONSHIP.FKCUST_HAS_AS_SECO';

comment on column CLC_INT_RELATIONSHIPS.CONTRACT_ID is 'Contract No -';

comment on column CLC_INT_RELATIONSHIPS.REL_TYPE is 'The type of relationship of Customer with specific account - 1.Business Party (BP)  Business Party (BP)RELATIONSHIP.FK_RELATIONSHIPTYPRELATIONSHIP_TYPE.TYPE_IDOther Types ???';

comment on column CLC_INT_RELATIONSHIPS.REL_RANK is 'Relationship Ranking -';

comment on column CLC_INT_RELATIONSHIPS.REL_STATUS is '(1) Valid, (0) Else -';

comment on column CLC_INT_RELATIONSHIPS.REL_GUARAMOUNT is '  (    15) -   (    15) -> AGREEMENT_GUARANT .GUARANTEE_AMOUNT';

comment on column CLC_INT_RELATIONSHIPS.REL_DATADATE is 'Date that data refer to - FILLED BY EXPORT APP';

comment on column CLC_INT_RELATIONSHIPS.REL_EXPORTDATE is 'Export date - FILLED BY EXPORT APP';

