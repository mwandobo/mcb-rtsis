create table PRF_ACC_SEQ
(
    SEQUENCE_TYPE   CHAR(10),
    ACCOUNT_SER_NUM DECIMAL(11)
);

create unique index IXU_PRF_006
    on PRF_ACC_SEQ (SEQUENCE_TYPE);

