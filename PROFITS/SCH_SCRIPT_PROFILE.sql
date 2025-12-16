create table SCH_SCRIPT_PROFILE
(
    FK_SCRIPT_ID VARCHAR(40)  not null,
    FK_PROFILE   VARCHAR(8)   not null,
    UPDATED_ON   TIMESTAMP(6) not null,
    UPDATED_BY   VARCHAR(20)  not null,
    constraint SCH_SCRIPT_PROFILE_PK
        primary key (FK_SCRIPT_ID, FK_PROFILE)
);

