create table DCD_SYSTEM
(
    ID          DECIMAL(12),
    SYSTEM_DESC CHAR(40)
);

create unique index IXP_DCD_002
    on DCD_SYSTEM (ID);

