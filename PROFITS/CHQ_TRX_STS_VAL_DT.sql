create table CHQ_TRX_STS_VAL_DT
(
    FK_TRANSACTION_ID  INTEGER not null,
    FK_INCOMING_STATUS CHAR(1) not null,
    STATUS             CHAR(1) not null,
    VALID_IND          CHAR(1)
);

create unique index IXU_DEP_101
    on CHQ_TRX_STS_VAL_DT (FK_TRANSACTION_ID, FK_INCOMING_STATUS, STATUS);

alter table CHQ_TRX_STS_VAL_DT
    add constraint IXU_REP_101
        primary key (FK_TRANSACTION_ID, FK_INCOMING_STATUS, STATUS);

