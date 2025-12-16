create table OUT_SYSTEM_BRIDGE
(
    TC_CODE         DECIMAL(10) not null
        constraint IXU_FX_022
            primary key,
    CHANNEL_ID      INTEGER,
    CREATION_DATE   DATE,
    ENTRY_STATUS    CHAR(1),
    VIRTUAL_USR_IND CHAR(1),
    EXTERNAL_SYSTEM CHAR(20),
    DESCRIPTION     CHAR(40)
);

