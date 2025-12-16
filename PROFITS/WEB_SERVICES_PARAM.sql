create table WEB_SERVICES_PARAM
(
    SN             SMALLINT     not null
        constraint IXU_CUS_049
            primary key,
    SN_SHORT_DESCR CHAR(10)     not null,
    PROXY_PASSWORD VARCHAR(20),
    PROXY_USERNAME VARCHAR(20),
    PROXY_PORT     INTEGER,
    PROXY_SERVER   VARCHAR(20),
    PORT           INTEGER      not null,
    URL            VARCHAR(512) not null,
    RETRY_MIN      SMALLINT     not null,
    MANDANT        VARCHAR(256),
    PASSWORD       VARCHAR(256),
    USERNAME       VARCHAR(256)
);

