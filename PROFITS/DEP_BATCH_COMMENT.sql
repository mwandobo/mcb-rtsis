create table DEP_BATCH_COMMENT
(
    PROGRAM_ID CHAR(5) not null
        constraint IXU_DEP_102
            primary key,
    COMMENTS   VARCHAR(200)
);

