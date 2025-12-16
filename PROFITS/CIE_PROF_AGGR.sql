create table CIE_PROF_AGGR
(
    FK_AGREEMENT_NO DECIMAL(10) not null,
    FK_PROFILE_ID   SMALLINT    not null,
    constraint IXU_DEF_113
        primary key (FK_AGREEMENT_NO, FK_PROFILE_ID)
);

