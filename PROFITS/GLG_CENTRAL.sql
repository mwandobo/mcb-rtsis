create table GLG_CENTRAL
(
    TRAN_ID             CHAR(6),
    FK_UNITCODE         INTEGER,
    DEACTIVATION_DATE   DATE,
    TIMESTMP            TIMESTAMP(6),
    VALIDITY_DATE       DATE,
    MOVMNT_TYPE         CHAR(1),
    CHANGE_STATUS_FLAG  CHAR(1),
    LEVEL0              CHAR(1),
    STATUS              CHAR(1),
    FK_GLG_DOCUMENTDO0  CHAR(2),
    FK_GLG_DOCUMENTDOC  CHAR(4),
    FK_GLG_JUSTIFYJUST  CHAR(4),
    FK_GLG_ACCOUNTACCO  CHAR(21),
    FK_GLG_ACCOUNTACC0  CHAR(21),
    ADMIN_NET_ACC       CHAR(21),
    DESCR               VARCHAR(30),
    FK_GH_HAS_GROUP_UNI CHAR(5),
    FK_GD_HAS_GROUP_UNI DECIMAL(5),
    MULT_UNIT_FLAG      CHAR(1) default '0' not null
);

create unique index IXU_GLG_055
    on GLG_CENTRAL (TRAN_ID);

