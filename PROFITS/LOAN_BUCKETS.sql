create table LOAN_BUCKETS
(
    BUCKET_ID SMALLINT,
    DAY_TO    SMALLINT,
    DAY_FROM  SMALLINT
);

create unique index IXU_LOA_041
    on LOAN_BUCKETS (BUCKET_ID);

