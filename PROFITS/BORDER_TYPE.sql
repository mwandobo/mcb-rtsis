create table BORDER_TYPE
(
    TIMESTAMP_IDENTIFI TIMESTAMP(6) not null
        constraint I0000394
            primary key,
    BORDER_RIGHT_WIDTH SMALLINT     not null,
    BORDER_BOTTOM_WIDT SMALLINT     not null,
    BORDER_TOP_WIDTH   SMALLINT     not null,
    BORDER_LEFT_WIDTH  SMALLINT     not null,
    BORDER_RIGHT       CHAR(2)      not null,
    BORDER_BOTTOM      CHAR(2)      not null,
    BORDER_TOP         CHAR(2)      not null,
    BORDER_LEFT        CHAR(2)      not null
);

