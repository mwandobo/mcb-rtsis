create table COS_MEMBER_ERROR_COUNT
(
    ERROR_ID_COUNTER DECIMAL(12) generated always as identity
        primary key,
    TIMESTAMP        TIMESTAMP(6)
);

