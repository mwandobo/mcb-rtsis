create table ART_LEGAL_ENTITIES
(
    CURRENT_DATE             DATE        not null,
    ENTITY_KEY               VARCHAR(30) not null,
    COUNTRY_CODE             CHAR(3),
    ID_TYPE                  CHAR(1),
    ID_NUMBER                VARCHAR(40),
    LEGAL_NAME               VARCHAR(100),
    NACE_CODE                CHAR(4),
    HAS_ECON_ACT_IN_REP      VARCHAR(2),
    ADDRESS_LINE_1           VARCHAR(100),
    ADDRESS_LINE_2           VARCHAR(100),
    POSTAL_CODE              VARCHAR(10),
    MUNICIPALITY_COMMUNITY   VARCHAR(20),
    DISTRICT                 VARCHAR(20),
    ADDRESS_COUNTRY          CHAR(2),
    CUSTOMER_INTERNAL_RATING VARCHAR(14),
    RATING_SCORING_DATE      DATE,
    COOPERATION_START_DATE   DATE,
    IS_INTRAGROUP_CUSTOMER   VARCHAR(2),
    IS_ACCOUNT_OWNER         CHAR(1),
    FILE_ACTION              CHAR(1) default 'F',
    primary key (CURRENT_DATE, ENTITY_KEY)
);

