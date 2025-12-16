create table ASSET_STATUS
(
    STATUS_CODE        VARCHAR(4) not null
        constraint IXU_GL_052
            primary key,
    INCLUDED           VARCHAR(1),
    DESCRIPTION        VARCHAR(40),
    INCLUDED_DISPOSALS VARCHAR(1) default '0'
);

