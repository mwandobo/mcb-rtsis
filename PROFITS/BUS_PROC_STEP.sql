create table BUS_PROC_STEP
(
    MODEL_ID          DECIMAL(10) not null,
    ID                DECIMAL(10) not null
        constraint IDC
            primary key,
    NAME              CHAR(32)    not null,
    ACTION_BLOCK_ID   DECIMAL(10) not null,
    SEQ               DECIMAL(10) not null,
    NON_SCREENED      CHAR(1)     not null,
    FK_BUSINESS_PROID DECIMAL(10),
    FK_MODELID        DECIMAL(10)
);

create unique index I0000536
    on BUS_PROC_STEP (FK_BUSINESS_PROID);

create unique index I0000573
    on BUS_PROC_STEP (FK_MODELID);

