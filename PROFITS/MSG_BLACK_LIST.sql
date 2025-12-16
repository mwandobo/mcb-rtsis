create table MSG_BLACK_LIST
(
    ID            DECIMAL(12) not null
        constraint IXM_BLL_001
            primary key,
    FK_CHANNEL_ID SMALLINT    not null,
    ADDRESS       VARCHAR(50) not null,
    DESCRIPTION   VARCHAR(50)
);

