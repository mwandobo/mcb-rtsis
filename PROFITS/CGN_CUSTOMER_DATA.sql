create table CGN_CUSTOMER_DATA
(
    CGN_SN             DECIMAL(10) generated always as identity,
    CUST_VIEW_SN       DECIMAL(10) not null,
    CUST_CUST_ID       INTEGER,
    SURNAME_SHORT      CHAR(10),
    LEADS              CHAR(1),
    LEADS_USER         CHAR(8),
    FIRST_NAME         VARCHAR(20),
    SURNAME            VARCHAR(70),
    TELEPHONE_1        VARCHAR(15),
    MOBILE_TEL         VARCHAR(15),
    E_MAIL             VARCHAR(64),
    ENTRY_COMMENTS     VARCHAR(254),
    ATTRACTION_DETAILS VARCHAR(40),
    constraint PK_CUST_LIST
        primary key (CUST_VIEW_SN, CGN_SN)
);

