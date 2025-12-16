create table SWIFT_QRY_CRITER
(
    GROUP_FACTOR       CHAR(5) not null,
    SN                 INTEGER not null,
    GROUP_STATUS       CHAR(2),
    GROUP_DESCRIPTION  CHAR(40),
    STATUS_DESC        CHAR(40),
    MSG_CATEGORY       CHAR(1),
    MSG_TYPE           CHAR(20),
    MSG_STATUS         CHAR(1),
    MSG_SUB_TYPE       VARCHAR(4),
    HDR_PROCESS_STATUS CHAR(1),
    HDR_ACCEPT_REJECT  CHAR(2),
    INC_ORDER_TYPE     CHAR(1),
    INC_ORD_STATUS     CHAR(1),
    constraint PK_SWIFT_STS_REP
        primary key (SN, GROUP_FACTOR)
);

