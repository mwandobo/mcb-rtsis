create table CHQ_TRX_STS_VAL_HD
(
    ID_TRANSACT     INTEGER not null,
    INCOMING_STATUS CHAR(1) not null
);

create unique index IXU_DEP_100
    on CHQ_TRX_STS_VAL_HD (ID_TRANSACT, INCOMING_STATUS);

alter table CHQ_TRX_STS_VAL_HD
    add constraint IXU_REP_100
        primary key (ID_TRANSACT, INCOMING_STATUS);

