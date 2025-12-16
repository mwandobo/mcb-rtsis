create table HUB_AML_RESULT
(
    LOGICAL_KEY VARCHAR(200) not null
        constraint HUB_AML_RESULT_PK
            primary key,
    STATUS      SMALLINT     not null,
    REASON      VARCHAR(4000),
    TIMESTMP    TIMESTAMP(6) default CURRENT TIMESTAMP
);

