create table CUST_MEC_GROUPS
(
    MEC_GROUP_ID       DECIMAL(10) not null,
    FK_GROUP           INTEGER     not null,
    FK_MEC             DECIMAL(10) not null,
    MEC_GROUP_DESCR    VARCHAR(40),
    MEC_GROUP_ANALYSIS LONG VARCHAR(32700),
    MEC_GROUP_STATUS   CHAR(1),
    INSERT_UNIT        INTEGER,
    INSERT_USR         CHAR(8),
    INSERT_DT          DATE,
    INSERT_STAMP       TIMESTAMP(6),
    UPDATE_UNIT        INTEGER,
    UPDATE_USR         CHAR(8),
    UPDATE_DT          DATE,
    UPDATE_STAMP       TIMESTAMP(6),
    constraint PK_CUST_MEC_GROUP
        primary key (FK_GROUP, FK_MEC, MEC_GROUP_ID)
);

create unique index PK_CUST_MEC_GROUP_2
    on CUST_MEC_GROUPS (FK_GROUP);

create unique index PK_CUST_MEC_GROUP_3
    on CUST_MEC_GROUPS (FK_MEC);

