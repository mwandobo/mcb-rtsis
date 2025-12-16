create table W_DIM_CUSTOMER_PROFILE
(
    PROFILE_KEY         INTEGER not null
        constraint PK_W_DIM_CUSTOMER_PROFILE
            primary key,
    SEGMENT_IND_NAME    VARCHAR(40),
    SEGMENT_IND         VARCHAR(2),
    SUBSEGMENT_IND_NAME VARCHAR(40),
    SUBSEGMENT_IND      VARCHAR(2)
);

