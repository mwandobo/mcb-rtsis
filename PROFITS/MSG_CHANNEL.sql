create table MSG_CHANNEL
(
    ID                SMALLINT           not null
        constraint IXM_CHN_001
            primary key,
    LABEL             VARCHAR(20)        not null,
    DESCRIPTION       VARCHAR(255),
    STATUS            SMALLINT default 0,
    HYPERCHANNEL      SMALLINT default 0 not null,
    MAX_CHAR_COUNT    INTEGER  default 0 not null,
    ALLOWS_ATTACHMENT SMALLINT default 0 not null,
    PROVIDER          VARCHAR(2000)
);

