create table GLG_INTFC_REL_UNIT
(
    SYS_UNIT_CODE   INTEGER not null
        constraint IXU_GL_008
            primary key,
    PRFT_UNIT_CODE  INTEGER,
    TIMESTMP        TIMESTAMP(6),
    RPLC_ON_ACC_PRC CHAR(1),
    SYS_UNIT_DESCR  VARCHAR(40)
);

