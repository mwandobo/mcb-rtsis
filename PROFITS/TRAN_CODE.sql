create table TRAN_CODE
(
    TRAN_ID  DECIMAL(11) not null
        constraint IXU_DEF_139
            primary key,
    TIMESTMP TIMESTAMP(6)
);

