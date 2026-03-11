-- auto-generated definition
create table COMPLAINT_REGISTER
(
    COMPLAINT_ID        INTEGER generated always as identity
        constraint PK_COMPLAINT_REGISTER
            primary key,
    CATEGORY            VARCHAR(50),
    ACCOUNT_NUMBER      VARCHAR(20),
    CUSTOMER_NAME       VARCHAR(100),
    CONTACT_NUMBER      VARCHAR(20),
    REGION              VARCHAR(50),
    LOGGED_BY           VARCHAR(50),
    LOGGED_DATE         DATE,
    RECEIVED_VIA        VARCHAR(30),
    CASE_DESCRIPTION    VARCHAR(2000),
    NATURE_OF_COMPLAINT VARCHAR(100),
    SERVICE_NAME        VARCHAR(100),
    WORKING_AGE         SMALLINT,
    RESOLUTION_INFO     VARCHAR(2000),
    STATUS              VARCHAR(20),
    CLOSING_DATE        DATE,
    TAT_STATUS          VARCHAR(20),
    CUSTOMER_SEGMENT    VARCHAR(50),
    QUERY_OWNER         VARCHAR(50),
    TMSTAMP             TIMESTAMP(6) default CURRENT TIMESTAMP not null
);

create unique index IDX_COMPLAINT_ACCOUNT
    on COMPLAINT_REGISTER (ACCOUNT_NUMBER);

create unique index IDX_COMPLAINT_DATE
    on COMPLAINT_REGISTER (LOGGED_DATE);

