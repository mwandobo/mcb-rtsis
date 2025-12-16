create table CUS_DTL_HISTORY_U
(
    FK_CUSTOMERCUST_ID INTEGER     not null,
    REC_ID             DECIMAL(15) not null,
    TMSTAMP            TIMESTAMP(6),
    ANNUALINCOME       DECIMAL(15, 2),
    RATAPPROVALNO      CHAR(20),
    CLASSIF_DATE       DATE,
    CLASSIFICATION     INTEGER,
    CLASSIF_SCALE      INTEGER,
    CUST_CATEGORY      INTEGER,
    UPDATE_USER        CHAR(8),
    constraint IXU_CIU_039
        primary key (REC_ID, FK_CUSTOMERCUST_ID)
);

