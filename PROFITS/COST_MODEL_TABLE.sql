create table COST_MODEL_TABLE
(
    TRN_ID CHAR(6)
);

create unique index IXU_COS_021
    on COST_MODEL_TABLE (TRN_ID);

