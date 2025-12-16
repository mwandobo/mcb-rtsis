create table PROFITS_ACC_CUST_LNK
(
    FK_SYSTEM      SMALLINT not null,
    FK_ACCOUNT     CHAR(40) not null,
    FK_CUSTOMER    INTEGER  not null,
    LINK_SN        INTEGER  not null,
    ADDRESS_SN     SMALLINT not null,
    RECIPIENT_FLAG CHAR(1),
    CREATE_USER    CHAR(8),
    CREATE_UNIT    INTEGER,
    CREATE_DATE    DATE,
    UPDATE_USER    CHAR(8),
    UPDATE_UNIT    INTEGER,
    UPDATE_DATE    DATE,
    LINK_STATUS    CHAR(1),
    LINK_COMMENTS  VARCHAR(2048),
    FK_GH_LINK     CHAR(5),
    FK_GD_LINK     INTEGER,
    constraint PK_ACCLINK
        primary key (FK_SYSTEM, FK_ACCOUNT, FK_CUSTOMER, LINK_SN)
);

