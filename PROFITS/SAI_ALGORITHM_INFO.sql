create table SAI_ALGORITHM_INFO
(
    ALGORITHM_ID SMALLINT not null
        constraint PK_SAI_ALGORITHM_INFO
            primary key,
    NAME         VARCHAR(200),
    DESCRIPTION  VARCHAR(2500)
);

