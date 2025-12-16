create table PAT_VERCONT_EXECUTION
(
    RUN_DATE                       DATE    not null,
    RUN_SEQ_ID                     INTEGER not null,
    CATEGORY_OF_THE_VERSION_CONTRO CHAR(1) not null,
    PROFITS_VERSION_DATE_FROM      TIMESTAMP(6),
    PROFITS_VERSION_DATE_TO        CHAR(13),
    DESCRIPTION                    VARCHAR(2000),
    constraint PATVCPK1
        primary key (RUN_SEQ_ID, RUN_DATE)
);

