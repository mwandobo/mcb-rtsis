create table MSG_BANKS
(
    ID          SMALLINT    not null
        constraint MSG_BANKS_PK
            primary key,
    LABEL       VARCHAR(40) not null,
    DESCRIPTION VARCHAR(200)
);

