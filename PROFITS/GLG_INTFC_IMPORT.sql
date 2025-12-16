create table GLG_INTFC_IMPORT
(
    FILENAME     VARCHAR(20) not null
        constraint IXU_GL_007
            primary key,
    TIMESTMP     TIMESTAMP(6),
    ENTRY_STATUS CHAR(1)
);

