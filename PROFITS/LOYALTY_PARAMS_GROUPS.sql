create table LOYALTY_PARAMS_GROUPS
(
    TAG_SET_CODE CHAR(20) not null,
    PARENT_TAG   CHAR(10) not null,
    SUB_TAG      CHAR(10) not null,
    constraint DEP_LOYAL_GROUPS_PK
        primary key (TAG_SET_CODE, SUB_TAG, PARENT_TAG)
);

