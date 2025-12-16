create table SUBS_KYA_REL_TMP
(
    SN        INTEGER,
    FIELD_300 VARCHAR(300)
);

create unique index IXU_SUB_001
    on SUBS_KYA_REL_TMP (SN);

