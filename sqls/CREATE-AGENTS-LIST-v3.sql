-- auto-generated definition
create table AGENTS_LIST_V3
(
    AGENT_NAME  CHAR(100)     null,
    TERMINAL_ID     CHAR(20)     null,
    AGENT_ID     CHAR(20)   not  null,
    TILL_NUMBER    CHAR(20)    null,
    BUSINESS_FORM   CHAR(20)  null,
    AGENT_PRINCIPAL  CHAR(100)     null,
    AGENT_PRINCIPAL_NAME  CHAR(100)     null,
    GENDER  CHAR(50)     null,
    REGISTRATION_DATE  CHAR(50)     null,
    CLOSED_DATE  CHAR(50)     null,
    CERT_INCORPORATION  CHAR(200)     null,
    NATIONALITY  CHAR(200)     null,
    AGENT_STATUS  CHAR(50)     null,
    AGENT_TYPE  CHAR(200)     null,
    ACCOUNT_NUMBER  CHAR(50)     null,
    O_REGION   CHAR(200)  null,
    REGION   CHAR(200)  null,
    O_DISTRICT   CHAR(200)  null,
    DISTRICT   CHAR(200)  null,
    O_WARD   CHAR(200)  null,
    WARD   CHAR(200)  null,
    STREET   CHAR(100)  null,
    HOUSE_NUMBER   CHAR(100)  null,
    POSTAL_CODE   CHAR(100)  null,
    COUNTRY   CHAR(100)  null,
    GPS_COORDINATES   CHAR(100)  null,
    AGENT_TAX_IDENTIFICATION_NUMBER   CHAR(100)  null,
    BUSINESS_LICENCE   CHAR(100)  null,
    IS_ACTIVE       CHAR(1) DEFAULT 1

);