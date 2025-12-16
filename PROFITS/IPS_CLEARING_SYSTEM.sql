create table IPS_CLEARING_SYSTEM
(
    CSM_CODE         VARCHAR(11) not null
        constraint PK_IPS_CLEARING_SYSTEM
            primary key,
    CSM_NAME         VARCHAR(35),
    CORRESPONDENT_ID INTEGER
);

