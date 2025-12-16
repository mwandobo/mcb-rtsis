create table SCH_TEMPLATE
(
    NAME     VARCHAR(100) not null
        constraint SCH_TEMPLATE_PK
            primary key,
    TEMPLATE BLOB(104857600)
);

