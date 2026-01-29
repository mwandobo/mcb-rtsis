-- auto-generated definition
create table AGENTS_LIST
(
    TERMINAL_ID    CHAR(20)    null,
    AGENT_ID     CHAR(20)     null,
    AGENT_NAME  CHAR(100)   not  null,
    AGENT_BUSINESS  CHAR(100)     null,
    BUSINESS_FORM   CHAR(20)  null,
    REGION   CHAR(20)  null,
    DISTRICT   CHAR(20)  null,
    LOCATION   CHAR(100)  null,
    GPS   CHAR(20)  null,
    PHONE   CHAR(20)  null,
    TIN   CHAR(20)  null,
    CERT_IN_CORPORATION   CHAR(200)  null,
    BUSINESS_LICENCE_ISSUER_AND_DATE   CHAR(200)  null,
    SHAREHOLDER_NAME  CHAR(100)  null,
    SHAREHOLDER_CARD  CHAR(100)   null,
    NAME_OF_AGENT_OPERATOR  CHAR(100)   null,
    PHONE_OF_AGENT_OPERATOR  CHAR(100)   null,
    EDUCATION_OF_AGENT_OPERATOR  CHAR(100)    null,
    BUSINESS_EXPERIENCE_OF_AGENT_OPERATOR  CHAR(100)   null,
    TMSTAMP           TIMESTAMP(6),
    constraint AGENTS_LIST_PK
        primary key (AGENT_NAME)
);

