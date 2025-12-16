create table IFRS9_ACCOUNT_DETAIL
(
    REFERENCE_DTM       DATE     not null,
    ACCOUNT_ID          CHAR(32) not null,
    CURRENT_MATURITY_DT TIMESTAMP(6),
    INTEREST_RT         DECIMAL(15, 2),
    primary key (REFERENCE_DTM, ACCOUNT_ID)
);

