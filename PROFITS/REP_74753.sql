create table REP_74753
(
    CREATION_DT  DATE        not null,
    ACC_UNIT     INTEGER     not null,
    ACC_TYPE     SMALLINT    not null,
    ACC_SN       INTEGER     not null,
    LOAN_STATUS  CHAR(1)     not null,
    ACC_CD       SMALLINT,
    GL_ACCOUNT   CHAR(21),
    PRODUCT      INTEGER,
    CUST_ID      INTEGER,
    CUST_CD      SMALLINT,
    POSITIVE_AMN DECIMAL(15, 2),
    UNIT_NAME    VARCHAR(40) not null
);

create unique index IXU_REP_022
    on REP_74753 (ACC_UNIT, CREATION_DT, LOAN_STATUS, ACC_SN, ACC_TYPE);

alter table REP_74753
    add constraint IXU_IXU_REP_022
        primary key (ACC_UNIT, CREATION_DT, LOAN_STATUS, ACC_SN, ACC_TYPE);

