create table GLG_INTFC_REL_ACC
(
    SYS_GLG_ACC      VARCHAR(40) not null
        constraint IXU_GL_057
            primary key,
    TIMESTMP         TIMESTAMP(6),
    PRFT_GLG_ACCOUNT CHAR(21)
);

