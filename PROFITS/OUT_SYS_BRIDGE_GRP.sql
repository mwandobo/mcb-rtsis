create table OUT_SYS_BRIDGE_GRP
(
    TC_CODE         DECIMAL(10) not null,
    INTERNAL_SN     INTEGER     not null,
    ATTRIBUTE_NAME  VARCHAR(18),
    ATTRIBUTE_VALUE VARCHAR(40),
    constraint IXU_FX_055
        primary key (TC_CODE, INTERNAL_SN)
);

