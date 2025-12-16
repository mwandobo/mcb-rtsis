create table CUSTOMER_SCORING
(
    CUST_ID    INTEGER not null,
    PROGRAM_ID CHAR(5) not null,
    constraint PR_CUST_SCORE
        primary key (PROGRAM_ID, CUST_ID)
);

