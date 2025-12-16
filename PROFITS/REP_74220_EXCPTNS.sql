create table REP_74220_EXCPTNS
(
    GL_ACC_TYPE     CHAR(1)  not null,
    PROFITS_ACC_NUM CHAR(40) not null,
    PROFITS_ACC_CD  SMALLINT,
    COMMENTS        VARCHAR(200),
    FAMOUNT         DECIMAL(15, 2),
    DEP_ACCOUNT     DECIMAL(11) default 0
);

