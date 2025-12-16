create table CHEQUE_DISC_DET
(
    FK_CHEQUE_DISC_OD  DECIMAL(11) not null,
    FK_CHEQUE_DISC_IDE INTEGER     not null,
    INTERNAL_SN        DECIMAL(10) not null,
    CANCEL_STATUS      SMALLINT,
    CHEQUE_BANK        SMALLINT,
    CHQ_AC_C_DIGIT     SMALLINT,
    COLL_BANK          SMALLINT,
    CUST_CODE          INTEGER,
    NOT_DISC_AMT       DECIMAL(15, 2),
    CHEQUE_ACCOUNT     DECIMAL(15),
    CHEQUE_AMOUNT      DECIMAL(15, 2),
    DISCOUNT_AMOUNT    DECIMAL(15, 2),
    TMSTAMP            DATE,
    MATURITY_DATE      DATE,
    STATUS             CHAR(1),
    CHEQUE_NUMBER      CHAR(10),
    COMMENTS           CHAR(30),
    constraint IXU_DEP_154
        primary key (FK_CHEQUE_DISC_OD, FK_CHEQUE_DISC_IDE, INTERNAL_SN)
);

