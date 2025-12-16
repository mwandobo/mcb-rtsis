create table USR_GRP_UNITS_REL
(
    USERCODE CHAR(8) not null,
    GROUP_ID INTEGER not null,
    constraint IXU_USR_005
        primary key (USERCODE, GROUP_ID)
);

