create table CLC_CONTACT
(
    CASE_ID           CHAR(40) not null,
    CLC_CONTACT_SN    INTEGER  not null,
    RECORD_DESCR      CHAR(70),
    RECORD_DETAILS    VARCHAR(4000),
    CUST_ID           INTEGER,
    CUST_CD           SMALLINT,
    USER_CODE         CHAR(8),
    AGENT_ID          DECIMAL(10),
    CREATE_UNIT       INTEGER,
    CREATE_DATE       DATE,
    CREATE_USER       CHAR(8),
    CREATE_TMSTAMP    TIMESTAMP(6),
    UPDATE_UNIT       INTEGER,
    UPDATE_DATE       DATE,
    UPDATE_USER       CHAR(8),
    UPDATE_TMSTAMP    TIMESTAMP(6),
    USER_CUSTOMER_FLG CHAR(1),
    CONTACT_TYPE      CHAR(2),
    CONTACT_STATUS    CHAR(1),
    constraint CLC_COLLECT_PK_8
        primary key (CASE_ID, CLC_CONTACT_SN)
);

