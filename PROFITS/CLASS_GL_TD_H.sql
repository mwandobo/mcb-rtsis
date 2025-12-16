create table CLASS_GL_TD_H
(
    FK_PRODUCTID_PRODU INTEGER not null,
    FK_GEN_DET_FINSC   CHAR(5) not null,
    FK_GEN_DET_FINSCV  INTEGER not null,
    FK_GENERIC_DETAFK  CHAR(5) not null,
    FK_GENERIC_DETASER INTEGER not null,
    FK_CUST_CATEG_GH   CHAR(5) not null,
    FK_CUST_CATEG_GD   INTEGER not null,
    FK_GLG_H_CURR_GGRO CHAR(4) not null,
    ACCNT_STATUS       CHAR(1) not null,
    ACCNT_CLASS        CHAR(1) not null,
    DURATION_DAYS_TO   INTEGER not null,
    TIMESTMP           TIMESTAMP(6),
    ENTRY_STATUS       CHAR(1),
    COMMENTS           VARCHAR(200),
    constraint PKCLASSGLTDH
        primary key (FK_PRODUCTID_PRODU, FK_GEN_DET_FINSC, FK_GEN_DET_FINSCV, FK_GENERIC_DETAFK, FK_GENERIC_DETASER,
                     FK_CUST_CATEG_GH, FK_CUST_CATEG_GD, FK_GLG_H_CURR_GGRO, ACCNT_STATUS, ACCNT_CLASS,
                     DURATION_DAYS_TO)
);

