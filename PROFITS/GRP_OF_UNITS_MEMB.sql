create table GRP_OF_UNITS_MEMB
(
    GROUP_ID INTEGER not null,
    UNITCODE INTEGER not null,
    constraint IXU_USR_006
        primary key (GROUP_ID, UNITCODE)
);

