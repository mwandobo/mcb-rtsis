create table SWIFT_REF_FRM
(
    REF_FRM_CODE      CHAR(10) not null,
    REF_FRM_CATEG     CHAR(1)  not null,
    REF_FRM_ORDER_SN  SMALLINT not null,
    REF_FRM_EXCEPTION CHAR(1)  not null,
    REF_FRM_DESCR     CHAR(50) not null,
    constraint PK_SWT_REF_FRM
        primary key (REF_FRM_CODE, REF_FRM_CATEG, REF_FRM_ORDER_SN)
);

