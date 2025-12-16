create table BLK_ACT_PER_ROLE
(
    STATUS_ID_CURR CHAR(1) not null,
    STATUS_ID_NEXT CHAR(1) not null,
    FK_PROFILE     CHAR(8) not null,
    ENTRY_STATUS   CHAR(1),
    TMSTAMP        TIMESTAMP(6),
    constraint FK_ACT_PER_ROLE
        primary key (FK_PROFILE, STATUS_ID_NEXT, STATUS_ID_CURR)
);

comment on column BLK_ACT_PER_ROLE.ENTRY_STATUS is '0:Inactive1:Active';

