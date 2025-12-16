create table CUST_MEC_ROLES
(
    MEC_MEMB_ID       DECIMAL(10) not null,
    FK_MEC            DECIMAL(10) not null,
    MEC_MEMB_DESCR    VARCHAR(40),
    MEC_MEMB_ANALYSIS LONG VARCHAR(32700),
    MEC_MEMB_STATUS   CHAR(1),
    START_DATE        DATE,
    EXPIRY_DATE       DATE,
    INSERT_USR        CHAR(8),
    INSERT_UNIT       INTEGER,
    INSERT_DT         DATE,
    INSERT_STAMP      TIMESTAMP(6),
    UPDATE_USR        CHAR(8),
    UPDATE_UNIT       INTEGER,
    UPDATE_DT         DATE,
    UPDATE_STAMP      TIMESTAMP(6),
    FK_GH_ROLE        CHAR(5),
    FK_GD_ROLE        INTEGER,
    FK_GROUP_MEMBER   INTEGER,
    FK_CUSTOMER       INTEGER,
    constraint PK_CUST_MEC_MEMB
        primary key (FK_MEC, MEC_MEMB_ID)
);

create unique index PK_CUST_MEC_MEMB_2
    on CUST_MEC_ROLES (FK_GH_ROLE, FK_GD_ROLE);

create unique index PK_CUST_MEC_MEMB_3
    on CUST_MEC_ROLES (FK_GROUP_MEMBER, FK_CUSTOMER);

