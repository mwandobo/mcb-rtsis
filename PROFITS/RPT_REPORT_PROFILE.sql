create table RPT_REPORT_PROFILE
(
    FK_REPORT_ID INTEGER      not null,
    FK_PROFILE   VARCHAR(8)   not null,
    UPDATE_ON    TIMESTAMP(6) not null,
    UPDATED_BY   VARCHAR(20)  not null,
    constraint RPT_REPORT_PROFILE_PK
        primary key (FK_REPORT_ID, FK_PROFILE)
);

